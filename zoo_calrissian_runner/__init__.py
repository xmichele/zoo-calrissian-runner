import base64
import os
import uuid
from datetime import datetime

from cwl_wrapper.parser import Parser
from loguru import logger
from pycalrissian.context import CalrissianContext
from pycalrissian.execution import CalrissianExecution
from pycalrissian.job import CalrissianJob

from zoo_calrissian_runner.handlers import ExecutionHandler


class CalrissianRunnerExecutionHandler(ExecutionHandler):
    def __ini__(self):
        pass

    def get_pod_env_vars(self):
        return {"A": "1", "B": "1"}

    def get_pod_node_selector(self):
        return None

    def get_secrets(self):

        username = ""
        password = ""
        email = ""
        registry = "https://index.docker.io/v1/"

        auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")

        secret_config = {
            "auths": {
                registry: {
                    "username": username,
                    "password": password,
                    "email": email,
                    "auth": auth,
                },
                "registry.gitlab.com": {"auth": ""},  # noqa: E501
            }
        }

        return secret_config

    def handle_log(self):

        return super().handle_log()

    def handle_output(self):
        return super().handle_output()

    def handle_usage_report(self):
        return super().handle_usage_report()


class Workflow:
    def __init__(self, cwl, workflow_id):

        self.cwl = cwl
        self.workflow_id = workflow_id

    def get_workflow(self):

        for elem in self.cwl["$graph"]:
            if self.workflow_id in [elem["id"]]:
                return elem

    def get_workflow_inputs(self):

        inputs = []
        for inp in self.get_workflow()["inputs"]:
            inputs.append(inp)

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


class ZooCalrissianRunner:
    def __init__(self, cwl, zoo, conf, inputs, outputs):

        self.zoo = zoo
        self.conf = ZooConf(conf)
        self.inputs = ZooInputs(inputs)
        self.outputs = ZooOutputs(outputs)
        self.cwl = Workflow(cwl, self.conf.workflow_id)

        self.handler = CalrissianRunnerExecutionHandler()

        self.storage_class = os.environ.get("STORAGE_CLASS", "longhorn")
        self.monitor_interval = 30

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

        self.shorten_namespace(
            f"{self.conf.workflow_id}-{str(datetime.now().timestamp()).replace('.', '')}-{uuid.uuid4()}"
        )

    def update_status(self, progress):

        self.zoo.update_status(self.conf, progress)

    def get_workflow_id(self):

        return self.conf.workflow_id

    def get_processing_parameters(self):
        """Gets the processing parameters from the zoo inputs"""
        return self.inputs.get_processing_parameters()

    def get_workflow_inputs(self):
        """Returns the CWL worflow inputs"""
        return self.cwl.get_workflow_inputs()

    def execute(self):

        logger.info("execution started")
        self.update_status(2)

        logger.info("wrap CWL workfow with stage-in/out steps")
        wrapped_worflow = self.wrap()
        self.update_status(5)

        logger.info("create kubernetes namespace for Calrissian execution")

        # TODO how do we manage the secrets
        secret_config = self.handler.get_secrets()

        session = CalrissianContext(
            namespace=self.get_namespace_name(),
            storage_class=self.storage_class,
            volume_size=self.get_volume_size(),
            image_pull_secrets=secret_config,
        )

        self.update_status(10)

        logger.info("create Calrissian job")
        job = CalrissianJob(
            cwl=wrapped_worflow,
            params=self.get_processing_parameters(),
            runtime_context=session,
            cwl_entry_point=self.conf.workflow_id,
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

        logger.info("retrieve execution logs")
        self.handler.handle_log(execution.get_log())
        self.update_status(93)

        logger.info("retrieve usage report")
        self.handler.handle_usage_report(execution.get_usage_report())
        self.update_status(95)

        logger.info("retrieve outputs")
        output = execution.get_output()
        self.handler.handle_output(output)
        self.outputs["Result"]["value"] = output

        self.update_status(97)

        logger.info("clean-up kubernetes resources")
        session.dispose()

        self.update_status(100)

        return exit_value

    def wrap(self):

        workflow_id = self.get_workflow_id()

        wf = Parser(
            cwl=self.cwl,
            output=None,
            stagein=os.environ.get("WRAPPER_STAGE_IN", "/assets/stagein.cwl"),
            stageout=os.environ.get("WRAPPER_STAGE_OUT", "/assets/stageout.cwl"),
            maincwl=os.environ.get("WRAPPER_MAIN", "/assets/main.cwl"),
            rulez=os.environ.get("WRAPPER_RULES", "/assets/rules.yaml"),
            assets=None,
            workflow_id=workflow_id,
        )

        return wf
