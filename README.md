# polyspark
Run batch scripts on multiple versions of Apache Spark

## System Requirements
- Java 8, with an update number higher than 8u92
- Python 3.6 or higher
- Additional JARs and native libraries to enable some optional Spark features

## Usage
Make a copy of `script_template.py` and add your PySpark code to it. Then `cd` into the directory containing `polyspark.py` and run Python code like this:
```python
from polyspark import run_on_spark
run_on_spark('path/to/myscript.py', '2.0.0')
run_on_spark('path/to/myscript.py', '3.0.0')
```
Optionally pass keyword arguments to `run_on_spark()`
```python
run_on_spark('path/to/myscript.py', '2.0.0', foo='bar', baz='qux')
```
In your PySpark code, get these arguments from the dictionary `args`.

Supported Spark versions include:
- `2.0.0`
- `2.0.1`
- `2.0.2`
- `2.1.0`
- `2.1.1`
- `2.1.2`
- `2.1.3`
- `2.2.0`
- `2.2.1`
- `2.2.2`
- `2.2.3`
- `2.3.0`
- `2.3.1`
- `2.3.2`
- `2.3.3`
- `2.3.4`
- `2.4.0`
- `2.4.1`
- `2.4.2`
- `2.4.3`
- `2.4.4`
- `2.4.5`
- `2.4.6`
- `2.4.7`
- `3.0.0`
- `3.0.1`
