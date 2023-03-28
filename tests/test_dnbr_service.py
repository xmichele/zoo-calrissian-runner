import os
import unittest

from .dnbr.service import dnbr


class TestSentinel2DNBRService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["CR_USERNAME"] = "fabricebrito"
        os.environ["CR_TOKEN"] = "dckr_pat_cVqA0dOTLkQi6XxDklSPpH91Qic"
        os.environ["CR_ENDPOINT"] = "https://index.docker.io/v1/"

        os.environ["AWS_SERVICE_URL"] = "https://s3.fr-par.scw.cloud"
        os.environ["AWS_REGION"] = "fr-par"
        os.environ["AWS_ACCESS_KEY_ID"] = "SCWEZ2E06RTJ8KAKCKBY"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "e13bf6e0-6278-4a96-a9d7-2a40575211cd"

        os.environ["KUBECONFIG"] = "/home/mambauser/.kube/kubeconfig-t2-dev.yaml"

        os.environ["STORAGE_CLASS"] = "openebs-kernel-nfs-scw"

        class ZooStub(object):
            def __init__(self):
                self.SERVICE_SUCCEEDED = 3
                self.SERVICE_FAILED = 4

            def update_status(self, conf, progress):
                print(f"Status {progress}")

            def _(self, message):
                print(f"invoked _ with {message}")

        try:
            import zoo
        except ImportError:
            print("Not running in zoo instance")

            zoo = ZooStub()

        cls.zoo = zoo

        conf = {}
        conf["lenv"] = {"message": ""}
        conf["lenv"] = {"workflow_id": "dnbr"}
        conf["tmpPath"] = "/tmp"

        cls.conf = conf

        inputs = {
            "post_stac_item": {
                "value": "https://earth-search.aws.element84.com/v0/collections/sentinel-s2-l2a-cogs/items/S2B_53HPA_20210723_0_L2A"  # noqa: E501
            },  # noqa: E501
            "pre_stac_item": {
                "value": "https://earth-search.aws.element84.com/v0/collections/sentinel-s2-l2a-cogs/items/S2B_53HPA_20210703_0_L2A"  # noqa: E501
            },  # noqa: E501
            "aoi": {"value": "136.659,-35.96,136.923,-35.791"},
        }

        cls.inputs = inputs

        outputs = {"Result": {"value": ""}}

        cls.outputs = outputs

    def test_execution(self):
        exit_code = dnbr(conf=self.conf, inputs=self.inputs, outputs=self.outputs)

        self.assertEqual(exit_code, self.zoo.SERVICE_SUCCEEDED)
