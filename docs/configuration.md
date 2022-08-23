# Configuration

## Environment variables

### K8S

* `STORAGE_CLASS`: defines the k8s RWX storage class for Calrissian. Defaults to `longhorn`

### CWL Wrapper
* `WRAPPER_STAGE_IN`: CWL stage-in template for cwl-wrapper. Defaults to `/assets/stagein.yaml`
* `WRAPPER_STAGE_OUT`: CWL stage-out template for cwl-wrapper. Defaults to `/assets/stageout.yaml`
* `WRAPPER_MAIN`: cwl-wrapper main template. Defaults to `/assets/maincwl.yaml`
* `WRAPPER_RULES`: cwl-wrapper rules template Defaults to `/assets/rules.yaml`

### Calrissian

* `DEFAULT_VOLUME_SIZE`: default volume size for the RWX volume used by Calrissian. Defaults to `10G`
* `MAX_CORES`: maximum number of cores used by Calrissian pods. Defaults to `2`
* `MAX_RAM`: maximum RAM used by Calrissian pods. Defaults to `4G`
