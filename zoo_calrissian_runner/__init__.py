import os
import uuid
from datetime import datetime
from typing import Union

from cwl_utils.parser import load_document_by_yaml
from cwl_wrapper.parser import Parser
from loguru import logger
from pycalrissian.context import CalrissianContext
from pycalrissian.execution import CalrissianExecution
from pycalrissian.job import CalrissianJob

from zoo_calrissian_runner.handlers import ExecutionHandler


class Workflow:
    def __init__(self, cwl, workflow_id):

        self.raw_cwl = cwl
        self.cwl = load_document_by_yaml(cwl, "io://")
        self.workflow_id = workflow_id

    def get_workflow(self):

        ids = [elem.id.split("#")[-1] for elem in self.cwl]

        return self.cwl[ids.index(self.workflow_id)]

    def get_workflow_inputs(self, mandatory=False):

        inputs = []
        for inp in self.get_workflow().inputs:
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
        self.workflow_id = self.conf["lenv"]["workflow_id"]


class ZooInputs:
    def __init__(self, inputs):

        self.inputs = inputs

    def get_input_value(self, key):

        try:
            return self.inputs[key]["value"]
        except KeyError as exc:
            raise exc
        except TypeError:
            pass

    def get_processing_parameters(self):
        """Returns a list with the input parameters keys"""
        params = {}

        for key, value in self.inputs.items():
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

        if "Result" in self.outputs.keys():
            self.outputs["Result"]["value"] = value
        else:
            self.outputs["Result"] = {"value": value}


class ZooCalrissianRunner:
    def __init__(
        self, cwl, zoo, conf, inputs, outputs, execution_handler: Union[ExecutionHandler, None] = None
    ):

        self.zoo = zoo
        self.conf = ZooConf(conf)
        self.inputs = ZooInputs(inputs)
        self.outputs = ZooOutputs(outputs)
        self.cwl = Workflow(cwl, self.conf.workflow_id)

        self.handler = execution_handler

        self.storage_class = os.environ.get("STORAGE_CLASS", "longhorn")
        self.monitor_interval = 30
        self._namespace_name = None

    @staticmethod
    def shorten_namespace(value: str) -> str:

        while len(value) > 63:
            value = value[:-1]
            while value.endswith("-"):
                value = value[:-1]
        return value

    def get_volume_size(self) -> str:
        # TODO how to determine the "right" volume size
        return "10G"

    def get_max_cores(self) -> int:
        # TODO how many cores for the CWL execution?
        return 2

    def get_max_ram(self) -> str:
        # TODO how much cores for the CWL execution?
        return "4G"

    def get_namespace_name(self):

        if self._namespace_name is None:
            return self.shorten_namespace(
                f"{self.conf.workflow_id}-"
                f"{str(datetime.now().timestamp()).replace('.', '')}-{uuid.uuid4()}"
            )
        else:
            return self._namespace_name

    def update_status(self, progress):

        self.zoo.update_status(self.conf, progress)

    def get_workflow_id(self):

        return self.conf.workflow_id

    def get_processing_parameters(self):
        """Gets the processing parameters from the zoo inputs"""
        return self.inputs.get_processing_parameters()

    def get_workflow_inputs(self, mandatory=False):
        """Returns the CWL worflow inputs"""
        return self.cwl.get_workflow_inputs(mandatory=mandatory)

    def assert_parameters(self):
        # checks all mandatory processing parameters were sent
        return all(
            elem in list(self.get_processing_parameters().keys())
            for elem in self.get_workflow_inputs(mandatory=True)
        )

    def execute(self):

        if not (self.assert_parameters()):
            logger.error("Mandatory parameters missing")
            return self.zoo.SERVICE_FAILED

        logger.info("execution started")
        self.update_status(2)

        logger.info("wrap CWL workfow with stage-in/out steps")
        wrapped_worflow = self.wrap()
        self.update_status(5)

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
        self.update_status(10)

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

        self.update_status(18)

        logger.info("execution")
        execution = CalrissianExecution(job=job, runtime_context=session)
        execution.submit()

        execution.monitor(interval=self.monitor_interval)

        if execution.is_complete():
            logger.info("execution complete")

        if execution.is_succeeded():
            exit_value = self.zoo.SERVICE_SUCCEEDED
        else:
            exit_value = self.zoo.SERVICE_FAILED

        self.update_status(90)

        logger.info("handle outputs execution logs")
        output = execution.get_output()
        self.outputs.set_output(output)

        self.handler.handle_outputs(
            log=execution.get_log(),
            output=output,
            usage_report=execution.get_usage_report(),
        )

        self.update_status(97)

        logger.info("clean-up kubernetes resources")
        # session.dispose()

        self.update_status(100)

        return exit_value

    def wrap(self):

        workflow_id = self.get_workflow_id()
        print(workflow_id)
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
