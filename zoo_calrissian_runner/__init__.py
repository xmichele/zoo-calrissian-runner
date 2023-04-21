import os
import uuid
from ast import literal_eval
from datetime import datetime
from typing import Union

from cwl_utils.parser import load_document_by_yaml
from cwl_wrapper.parser import Parser
from loguru import logger
from pycalrissian.context import CalrissianContext
from pycalrissian.execution import CalrissianExecution
from pycalrissian.job import CalrissianJob

from zoo_calrissian_runner.handlers import ExecutionHandler

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()


class Workflow:
    def __init__(self, cwl, workflow_id):
        self.raw_cwl = cwl
        self.cwl = load_document_by_yaml(yaml=cwl, uri="io://", id_=workflow_id)
        self.workflow_id = workflow_id

    def get_workflow(self):
        ids = [elem.id.split("#")[-1] for elem in self.cwl]

        return self.cwl[ids.index(self.workflow_id)]

    def get_workflow_inputs(self, mandatory=False):
        inputs = []
        for inp in self.cwl.inputs:
            if mandatory:
                if inp.default is not None:
                    continue

                # test optional array of string
                elif inp.type != ["null", "string"]:
                    inputs.append(inp.id.split("/")[-1])
                else:
                    continue
            else:
                inputs.append(inp.id.split("/")[-1])
        return inputs


class ZooConf:
    def __init__(self, conf):
        self.conf = conf
        self.workflow_id = self.conf["lenv"]["Identifier"]


class ZooInputs:
    def __init__(self, inputs):
        self.inputs = inputs

    def get_input_value(self, key):
        try:
            if "isArray" in self.inputs[key] and self.inputs[key]["isArray"] == "true":
                return literal_eval(self.inputs[key]["value"])
            else:
                return self.inputs[key]["value"]

        except KeyError as exc:
            raise exc
        except TypeError:
            pass

    def get_processing_parameters(self):
        """Returns a list with the input parameters keys"""
        params = {}

        for key, value in self.inputs.items():
            if "isArray" in value and value["isArray"] == "true":
                params[key] = literal_eval(value["value"])
            else:
                params[key] = value["value"]

        return params


class ZooOutputs:
    def __init__(self, outputs):
        self.outputs = outputs

    def get_output_parameters(self):
        """Returns a list with the output parameters keys"""
        params = {}

        for key, value in self.outputs.items():
            params[key] = value["value"]

        return params

    def set_output(self, value):
        """set the output result value"""
        if "Result" in self.outputs.keys():
            self.outputs["Result"]["value"] = value
        else:
            self.outputs["Result"] = {"value": value}


class ZooCalrissianRunner:
    def __init__(
        self, cwl, conf, inputs, outputs, execution_handler: Union[ExecutionHandler, None] = None
    ):
        self.zoo_conf = ZooConf(conf)
        self.inputs = ZooInputs(inputs)
        self.outputs = ZooOutputs(outputs)
        self.cwl = Workflow(cwl, self.zoo_conf.workflow_id)

        self.handler = execution_handler

        self.storage_class = os.environ.get("STORAGE_CLASS", "longhorn")
        self.monitor_interval = 30
        self._namespace_name = None

    @staticmethod
    def shorten_namespace(value: str) -> str:
        """shortens the namespace to 63 characters"""
        while len(value) > 63:
            value = value[:-1]
            while value.endswith("-"):
                value = value[:-1]
        return value

    def get_volume_size(self) -> str:
        """returns volume size that the pods share"""
        # TODO how to determine the "right" volume size
        return os.environ.get("DEFAULT_VOLUME_SIZE", "10G")

    def get_max_cores(self) -> int:
        """returns the maximum number of cores that pods can use"""
        # TODO how many cores for the CWL execution?
        return os.environ.get("MAX_CORES", int("2"))

    def get_max_ram(self) -> str:
        """returns the maximum RAM that pods can use"""
        # TODO how much RAM for the CWL execution?
        return os.environ.get("MAX_RAM", "4G")

    def get_namespace_name(self):
        """creates or returns the namespace"""
        if self._namespace_name is None:
            return self.shorten_namespace(
                f"{self.zoo_conf.workflow_id}-"
                f"{str(datetime.now().timestamp()).replace('.', '')}-{uuid.uuid4()}"
            )
        else:
            return self._namespace_name

    def update_status(self, progress: int, message: str = None) -> None:
        """updates the exection progress (%) and provides an optional message"""
        if message:
            self.zoo_conf.conf["lenv"]["message"] = message

        zoo.update_status(self.zoo_conf.conf, progress)

    def get_workflow_id(self):
        """returns the workflow id (CWL entry point)"""
        return self.zoo_conf.workflow_id

    def get_processing_parameters(self):
        """Gets the processing parameters from the zoo inputs"""
        return self.inputs.get_processing_parameters()

    def get_workflow_inputs(self, mandatory=False):
        """Returns the CWL worflow inputs"""
        return self.cwl.get_workflow_inputs(mandatory=mandatory)

    def assert_parameters(self):
        """checks all mandatory processing parameters were provided"""
        return all(
            elem in list(self.get_processing_parameters().keys())
            for elem in self.get_workflow_inputs(mandatory=True)
        )

    def execute(self):
        if not (self.assert_parameters()):
            logger.error("Mandatory parameters missing")
            return zoo.SERVICE_FAILED

        logger.info("execution started")
        self.update_status(progress=2, message="starting execution")

        logger.info("wrap CWL workfow with stage-in/out steps")
        wrapped_worflow = self.wrap()
        self.update_status(progress=5, message="workflow wrapped, creating processing environment")

        logger.info("create kubernetes namespace for Calrissian execution")

        # TODO how do we manage the secrets
        secret_config = self.handler.get_secrets()

        namespace = self.get_namespace_name()

        self.handler.set_job_id(job_id=namespace)

        logger.info(f"namespace: {namespace}")

        session = CalrissianContext(
            namespace=namespace,
            storage_class=self.storage_class,
            volume_size=self.get_volume_size(),
            image_pull_secrets=secret_config,
        )
        session.initialise()
        self.update_status(progress=10, message="processing environment created, preparing execution")

        processing_parameters = {
            "process": namespace,
            **self.get_processing_parameters(),
            **self.handler.get_additional_parameters(),
        }

        # checks if all parameters where provided

        logger.info("create Calrissian job")
        job = CalrissianJob(
            cwl=wrapped_worflow,
            params=processing_parameters,
            runtime_context=session,
            cwl_entry_point="main",
            max_cores=self.get_max_cores(),
            max_ram=self.get_max_ram(),
            pod_env_vars=self.handler.get_pod_env_vars(),
            pod_node_selector=self.handler.get_pod_node_selector(),
            debug=True,
            no_read_only=True,
        )

        self.update_status(progress=18, message="execution submitted")

        logger.info("execution")
        execution = CalrissianExecution(job=job, runtime_context=session)
        execution.submit()

        execution.monitor(interval=self.monitor_interval)

        if execution.is_complete():
            logger.info("execution complete")

        if execution.is_succeeded():
            exit_value = zoo.SERVICE_SUCCEEDED
        else:
            exit_value = zoo.SERVICE_FAILED

        self.update_status(progress=90, message="delivering outputs, logs and usage report")

        logger.info("handle outputs execution logs")
        output = execution.get_output()
        self.outputs.set_output(output)

        self.handler.handle_outputs(
            log=execution.get_log(),
            output=output,
            usage_report=execution.get_usage_report(),
        )

        self.update_status(progress=97, message="clean-up processing resources")

        logger.info("clean-up kubernetes resources")
        session.dispose()

        self.update_status(
            progress=100,
            message="processing done, execution {}".format(
                "failed" if exit_value == zoo.SERVICE_FAILED else "successful"
            ),
        )

        return exit_value

    def wrap(self):
        workflow_id = self.get_workflow_id()

        wf = Parser(
            cwl=self.cwl.raw_cwl,
            output=None,
            stagein=os.environ.get("WRAPPER_STAGE_IN", "/assets/stagein.yaml"),
            stageout=os.environ.get("WRAPPER_STAGE_OUT", "/assets/stageout.yaml"),
            maincwl=os.environ.get("WRAPPER_MAIN", "/assets/maincwl.yaml"),
            rulez=os.environ.get("WRAPPER_RULES", "/assets/rules.yaml"),
            assets=None,
            workflow_id=workflow_id,
        )

        return wf.out
