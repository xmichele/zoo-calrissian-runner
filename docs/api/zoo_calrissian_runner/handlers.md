# Module zoo_calrissian_runner.handlers

None

None

## Classes

### ExecutionHandler

```python3
class ExecutionHandler(
    **kwargs
)
```

#### Ancestors (in MRO)

* abc.ABC

#### Methods


#### get_additional_parameters

```python3
def get_additional_parameters(
    self
)
```




#### get_pod_env_vars

```python3
def get_pod_env_vars(
    self
)
```




#### get_pod_node_selector

```python3
def get_pod_node_selector(
    self
)
```




#### get_secrets

```python3
def get_secrets(
    self
)
```




#### handle_outputs

```python3
def handle_outputs(
    self,
    execution_log,
    output,
    usage_report
)
```




#### set_job_id

```python3
def set_job_id(
    self,
    job_id
)
```
