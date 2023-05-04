# Module zoo_calrissian_runner

None

None

## Sub-modules

* [zoo_calrissian_runner.handlers](handlers/)

## Classes

### Workflow

```python3
class Workflow(
    cwl,
    workflow_id
)
```

#### Methods


#### get_workflow

```python3
def get_workflow(
    self
)
```




#### get_workflow_inputs

```python3
def get_workflow_inputs(
    self,
    mandatory=False
)
```



### ZooCalrissianRunner

```python3
class ZooCalrissianRunner(
    cwl,
    zoo,
    conf,
    inputs,
    outputs,
    execution_handler: Optional[zoo_calrissian_runner.handlers.ExecutionHandler] = None
)
```

#### Static methods


#### shorten_namespace

```python3
def shorten_namespace(
    value: str
) -> str
```



#### Methods


#### assert_parameters

```python3
def assert_parameters(
    self
)
```




#### execute

```python3
def execute(
    self
)
```




#### get_max_cores

```python3
def get_max_cores(
    self
) -> int
```




#### get_max_ram

```python3
def get_max_ram(
    self
) -> str
```




#### get_namespace_name

```python3
def get_namespace_name(
    self
)
```




#### get_processing_parameters

```python3
def get_processing_parameters(
    self
)
```


Gets the processing parameters from the zoo inputs


#### get_volume_size

```python3
def get_volume_size(
    self
) -> str
```




#### get_workflow_id

```python3
def get_workflow_id(
    self
)
```




#### get_workflow_inputs

```python3
def get_workflow_inputs(
    self,
    mandatory=False
)
```


Returns the CWL worflow inputs


#### update_status

```python3
def update_status(
    self,
    progress
)
```




#### wrap

```python3
def wrap(
    self
)
```



### ZooConf

```python3
class ZooConf(
    conf
)
```

### ZooInputs

```python3
class ZooInputs(
    inputs
)
```

#### Methods


#### get_input_value

```python3
def get_input_value(
    self,
    key
)
```




#### get_processing_parameters

```python3
def get_processing_parameters(
    self
)
```


Returns a list with the input parameters keys

### ZooOutputs

```python3
class ZooOutputs(
    outputs
)
```

#### Methods


#### get_output_parameters

```python3
def get_output_parameters(
    self
)
```


Returns a list with the output parameters keys


#### set_output

```python3
def set_output(
    self,
    value
)
```
