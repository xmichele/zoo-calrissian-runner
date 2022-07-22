import tempfile
import unittest

import yaml

from zoo_calrissian_runner import ZooCalrissianRunner


class TestCalrissianContext(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_output_file = tempfile.NamedTemporaryFile()

        try:
            import zoo
        except ImportError:
            print("Not running in zoo instance")

            class ZooStub(object):
                def __init__(self):
                    self.SERVICE_SUCCEEDED = 3
                    self.SERVICE_FAILED = 4

                def update_status(self, conf, progress):
                    print(f"Status {progress}")

                def _(self, message):
                    print(f"invoked _ with {message}")

            conf = {}
            conf["lenv"] = {"message": ""}
            zoo = ZooStub()

            cls.zoo = zoo

    def test_zoo_object(self):

        self.assertEquals(self.zoo.SERVICE_SUCCEEDED, 3)

    def test_object_creation(self):

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=None, outputs=None)

        self.assertIsInstance(runner, ZooCalrissianRunner)

    def test_get_input_invalid_value(self):

        inputs = {}

        inputs["_cwl"] = {"value": "value1"}
        inputs["_workflow_id"] = {"value": "dbnr"}
        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        with self.assertRaises(KeyError):
            runner.inputs.get_input_value("missing_key")

    def test_get_input_value(self):

        inputs = {}

        inputs["_cwl"] = {"value": "a value"}
        inputs["_workflow_id"] = {"value": "dbnr"}
        inputs["input_1"] = {"value": "value1"}

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        self.assertEquals(runner.inputs.get_input_value("input_1"), "value1")

    def test_load_real_cwl(self):

        with open("app-packages/dNBR.cwl", "r") as stream:
            cwl = yaml.safe_load(stream)

        inputs = {}

        inputs["_cwl"] = {"value": cwl}
        inputs["_workflow_id"] = {"value": "dbnr"}
        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        self.assertEquals(cwl, runner.get_cwl())

    def test_wrapper(self):

        with open("app-packages/dNBR.cwl", "r") as stream:
            cwl = yaml.safe_load(stream)

        inputs = {}

        inputs["_cwl"] = {"value": cwl}
        inputs["_workflow_id"] = {"value": "dbnr"}

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        runner.wrap()

    def test_get_processing_parameters(self):

        with open("app-packages/dNBR.cwl", "r") as stream:
            cwl = yaml.safe_load(stream)

        inputs = {}

        inputs["_cwl"] = {"value": cwl}
        inputs["_workflow_id"] = {"value": "dbnr"}
        inputs["param_1"] = {"value": "value1"}
        inputs["param_2"] = {"value": "value2"}

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        params = {"param_1": "value1", "param_2": "value2"}

        self.assertEquals(params, runner.get_processing_parameters())

    def test_get_workflow(self):

        with open("app-packages/dNBR.cwl", "r") as stream:
            cwl = yaml.safe_load(stream)

        ref_cwl = {
            "class": "Workflow",
            "label": "dNBR - produce the delta normalized difference between NIR and SWIR 22 over a pair of stac items",  # noqa: E501
            "doc": "dNBR - produce the delta normalized difference between NIR and SWIR 22 over a pair of stac items",  # noqa: E501
            "id": "dnbr",
            "requirements": [
                {"class": "ScatterFeatureRequirement"},
                {"class": "SubworkflowFeatureRequirement"},
                {"class": "MultipleInputFeatureRequirement"},
            ],
            "inputs": {
                "pre_stac_item": {"doc": "Pre-event Sentinel-2 item", "type": "string"},
                "post_stac_item": {
                    "doc": "Post-event Sentinel-2 item",
                    "type": "string",
                },
                "aoi": {"doc": "area of interest as a bounding box", "type": "string"},
                "bands": {"type": "string[]", "default": ["B8A", "B12", "SCL"]},
            },
            "outputs": {"stac": {"outputSource": ["node_stac/stac"], "type": "Directory"}},
            "steps": {
                "node_nbr": {
                    "run": "#nbr_wf",
                    "in": {
                        "stac_item": ["pre_stac_item", "post_stac_item"],
                        "aoi": "aoi",
                    },
                    "out": ["nbr"],
                    "scatter": "stac_item",
                    "scatterMethod": "dotproduct",
                },
                "node_dnbr": {
                    "run": "#dnbr_clt",
                    "in": {"tifs": {"source": "node_nbr/nbr"}},
                    "out": ["dnbr"],
                },
                "node_cog": {
                    "run": "#gdal_cog_clt",
                    "in": {"tif": {"source": ["node_dnbr/dnbr"]}},
                    "out": ["cog_tif"],
                },
                "node_stac": {
                    "run": "#stacme_clt",
                    "in": {
                        "tif": {"source": ["node_cog/cog_tif"]},
                        "pre_stac_item": "pre_stac_item",
                        "post_stac_item": "post_stac_item",
                    },
                    "out": ["stac"],
                },
            },
        }
        inputs = {}

        inputs["_cwl"] = {"value": cwl}
        inputs["_workflow_id"] = {"value": "dnbr"}

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        self.assertEquals(ref_cwl, runner.cwl.get_workflow())

    def test_get_workflow_inputs(self):

        with open("app-packages/dNBR.cwl", "r") as stream:
            cwl = yaml.safe_load(stream)

        inputs = {}

        inputs["_cwl"] = {"value": cwl}
        inputs["_workflow_id"] = {"value": "dnbr"}

        runner = ZooCalrissianRunner(zoo=self.zoo, conf=None, inputs=inputs, outputs=None)

        self.assertEquals(
            set(["pre_stac_item", "post_stac_item", "aoi", "bands"]),
            set(runner.cwl.get_workflow_inputs()),
        )
