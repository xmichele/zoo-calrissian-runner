# Configuration

## Environment variables

### K8S

* `STORAGE_CLASS`: defines the k8s RWX storage class for Calrissian. Defaults to `longhorn`

### CWL Wrapper

The cwl-wrapper templates can be customized with the eenvironment variables:

* `WRAPPER_STAGE_IN`: CWL stage-in template for cwl-wrapper. Defaults to `/assets/stagein.yaml`
* `WRAPPER_STAGE_OUT`: CWL stage-out template for cwl-wrapper. Defaults to `/assets/stageout.yaml`
* `WRAPPER_MAIN`: cwl-wrapper main template. Defaults to `/assets/maincwl.yaml`
* `WRAPPER_RULES`: cwl-wrapper rules template Defaults to `/assets/rules.yaml`

### Calrissian

Calrissian and its runtime context can be customized with:

* `CALRISSIAN_IMAGE`: Calrissian container image
* `DEFAULT_VOLUME_SIZE`: default volume size for the RWX volume used by Calrissian. Expressed in mebibytes (2**20). Defaults to `10000`
* `DEFAULT_MAX_CORES`: maximum number of cores used by Calrissian pods. Defaults to `2`
* `DEFAULT_MAX_RAM`: maximum RAM used by Calrissian pods.
