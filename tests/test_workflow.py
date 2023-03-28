import unittest
import os
import yaml
from zoo_calrissian_runner import Workflow
import cwl_utils


class TestWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(
            os.path.join("tests", "app-packages", "app-package-1.cwl"),
            "r",
        ) as stream:
            cls.reference_wf1 = {"cwl": yaml.safe_load(stream), "workflow_id": "dnbr"}

        with open(
            os.path.join("tests", "app-packages", "app-package-2.cwl"),
            "r",
        ) as stream:
            cls.reference_wf2 = {"cwl": yaml.safe_load(stream), "workflow_id": "dnbr"}

        with open(
            os.path.join("tests", "app-packages", "app-package-3.cwl"),
            "r",
        ) as stream:
            cls.reference_wf3 = {"cwl": yaml.safe_load(stream), "workflow_id": "dnbr"}

        with open(
            os.path.join("tests", "app-packages", "app-package-4.cwl"),
            "r",
        ) as stream:
            cls.reference_wf4 = {"cwl": yaml.safe_load(stream), "workflow_id": "dnbr"}

    def test_object_creation(self):
        workflow = Workflow(cwl=self.reference_wf1["cwl"], workflow_id=self.reference_wf1["workflow_id"])

        self.assertIsInstance(workflow, Workflow)

    def test_get_workflow(self):
        workflow = Workflow(cwl=self.reference_wf1["cwl"], workflow_id=self.reference_wf1["workflow_id"])

        self.assertIsInstance(workflow.get_workflow(), cwl_utils.parser.cwl_v1_2.Workflow)

    def test_workflow_hints(self):
        workflow = Workflow(cwl=self.reference_wf4["cwl"], workflow_id=self.reference_wf4["workflow_id"])

        self.assertEqual(
            {"coresMin": [3], "coresMax": [], "ramMin": [10240], "ramMax": []},
            workflow.eval_resource(),
        )

    def test_workflow_requirements(self):
        workflow = Workflow(cwl=self.reference_wf2["cwl"], workflow_id=self.reference_wf2["workflow_id"])

        self.assertEqual(
            {"coresMin": [3], "coresMax": [], "ramMin": [10240], "ramMax": []},
            workflow.eval_resource(),
        )

    def test_clt_requirements(self):
        workflow = Workflow(cwl=self.reference_wf3["cwl"], workflow_id=self.reference_wf3["workflow_id"])

        self.assertEqual(
            {
                "coresMin": [3, 3, 6, 3, 3, 3, 3, 6, 6],
                "coresMax": [],
                "ramMin": [10240, 10240, 20480, 10240, 10240, 10240, 10240, 20480, 20480],
                "ramMax": [],
            },
            workflow.eval_resource(),
        )

    def test_max_ram(self):
        workflow = Workflow(cwl=self.reference_wf3["cwl"], workflow_id=self.reference_wf3["workflow_id"])

        resources = workflow.eval_resource()
        max_ram = max(max(resources["ramMin"] or [0]), max(resources["ramMax"] or [0]))

        self.assertEqual(max_ram, 20480)

    def test_max_cores(self):
        workflow = Workflow(cwl=self.reference_wf3["cwl"], workflow_id=self.reference_wf3["workflow_id"])

        resources = workflow.eval_resource()
        max_cores = max(max(resources["coresMin"] or [0]), max(resources["coresMax"] or [0]))

        self.assertEqual(max_cores, 6)
