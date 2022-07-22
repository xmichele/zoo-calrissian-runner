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


class ZooInputs:
    def __init__(self, inputs):

        self.inputs = inputs
        self.reserved_keys = ["_cwl", "_workflow_id"]

    def get_cwl(self):
        return self.get_input_value("_cwl")

    def get_workflow_id(self):
        return self.get_input_value("_workflow_id")

    def get_input_value(self, key):

        try:
            return self.inputs[key]["value"]
        except KeyError as exc:
            raise exc
        except TypeError:
            pass

    def get_processing_parameters(self):

        params = {}

        for key, value in self.inputs.items():
            if key not in self.reserved_keys:
                params[key] = value["value"]

        return params


class ZooCalrissianRunner:
    def __init__(self, zoo, conf, inputs, outputs):

        self.zoo = zoo
        self.conf = conf
        self.inputs = ZooInputs(inputs)
        self.outputs = outputs
        self.cwl = Workflow(self.get_cwl(), self.get_workflow_id())

    def update_status(self, progress):

        self.zoo.update_status(self.conf, progress)

    def get_cwl(self):

        return self.inputs.get_cwl()

    def get_workflow_id(self):

        return self.inputs.get_workflow_id()

    def get_processing_parameters(self):

        return self.inputs.get_processing_parameters()

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

        cwl = self.inputs.get_cwl()
        workflow_id = self.inputs.get_workflow_id()

        wf = Parser(
            cwl=cwl,
            output=None,
            stagein="/assets/stagein.cwl",
            stageout="/assets/stageout.cwl",
            maincwl="/assets/main.cwl",
            rulez="/assets/rules.yaml",
            assets=None,
            workflow_id=workflow_id,
        )

        return wf
