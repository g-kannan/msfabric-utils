# msfabric-utils
Utilities for MS Fabric Python Notebooks &amp; Pipelines

## Installation
python setup.py bdist_wheel

## Usage
%pip install /lakehouse/default/Files/<Replace with your Location>/shared_utils-version-py3-none-any.whl 

## Example
from shared_utils.datalake_utils import *
get_delta_table_path("schema","table_name")

## Functions
get_time()
generate_random_uuid()
get_duckdb_conn()
read_lakehouse_files(duckdb_conn,data_format,file_path)
get_fabric_metadata(property)
get_delta_table_path(schema,table_name)
get_lakehouse_files(folder_path="/lakehouse/default/Files/", pattern=None)
run_notebook(job_config, params)
audit_run(job_config, file_to_process, exit_value, outcome)
move_trailing_minus(value)
