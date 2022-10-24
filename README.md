# zoo-calrissian-runner

Python library for bridging zoo execution context and calrissian

## Environment variables

* `STORAGE_CLASS`: RWX storage class
* `CALRISSIAN_IMAGE`: Calrissian container image
* `DEFAULT_VOLUME_SIZE`: default size for RWX storage volume
* `MAX_CORES`: maximum number of cores to use during a Calrissian Job
* `MAX_RAM`: maximum number of RAM to use during a Calrissian Job

CWL wrapper templates:

* `WRAPPER_STAGE_IN`
* `WRAPPER_STAGE_OUT`
* `WRAPPER_STAGE_MAIN`
* `WRAPPER_STAGE_RULES`
