from cwl_wrapper.parser import Parser


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

        self.update_status(2)

        self.wrap()

        # do something
        print("hello world!")

        self.update_status(20)

        print("again")

        self.update_status(100)

        self.outputs["Result"]["value"] = "a value"

        return True

    def wrap(self):

        workflow_id = self.get_workflow_id()

        wf = Parser(
            cwl=self.cwl,
            output=None,
            stagein="/assets/stagein.cwl",
            stageout="/assets/stageout.cwl",
            maincwl="/assets/main.cwl",
            rulez="/assets/rules.yaml",
            assets=None,
            workflow_id=workflow_id,
        )

        return wf
