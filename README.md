# zoo-calrissian-runner

Python library for bridging zoo execution context and calrissian

## Environment variables

* `STORAGE_CLASS`: RWX storage class
* `CALRISSIAN_IMAGE`: Calrissian container image
* `DEFAULT_VOLUME_SIZE`: default size for RWX storage volume
* `MAX_CORES`: maximum number of cores to use during a Calrissian Job to be used if the CWL does not set the resource requirements
* `MAX_RAM`: maximum number of RAM to use during a Calrissian Job to be used if the CWL does not set the resource requirements

CWL wrapper templates:

* `WRAPPER_STAGE_IN`
* `WRAPPER_STAGE_OUT`
* `WRAPPER_STAGE_MAIN`
* `WRAPPER_STAGE_RULES`

## Running the tests

Add a `tests/.env` file including the values with::

```
CR_USERNAME=""
CR_TOKEN=""
CR_ENDPOINT="https://index.docker.io/v1/"
CR_EMAIL=""
AWS_SERVICE_URL=""
AWS_REGION=""
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""

KUBECONFIG=""
STORAGE_CLASS=""

DEFAULT_MAX_CORES=8
DEFAULT_MAX_RAM=1024
DEFAULT_VOLUME_SIZE=10000 # mebibytes (2**20)


ADES_STAGEOUT_OUTPUT=""
```
