import duckdb
from deltalake import DeltaTable,write_deltalake
from datetime import datetime
import pytz
from loguru import logger
import uuid

def get_time():
    ist = pytz.timezone('Asia/Kolkata')
    ist_time = datetime.now(ist).strftime('%Y-%m-%dT%H:%M:%S')
    return ist_time

def generate_random_uuid():
    """
    Generates and returns a random UUID4 as a string.
    """
    return str(uuid.uuid4())

def get_duckdb_conn():
    """
    Returns a new connection to a duckdb in-memory database with the timezone set to Asia/Kolkata
    
    Returns:
    duckdb.Connection: a new connection to a duckdb database
    """
    conn = duckdb.connect(":memory:")
    conn.sql("SET TimeZone = 'Asia/Kolkata';")
    return conn

def read_lakehouse_data(duckdb_conn,data_format,file_path):
    """
    Reads data from a lakehouse file and returns it as a duckdb table with the two extra columns LOAD_TS and UPDATE_TS.
    
    Parameters:
    duckdb_conn (duckdb.Connection): an open connection to a duckdb database
    data_format (str): the format of the file, either 'csv' or 'xlsx'
    file_path (str): the path to the file to read
    
    Returns:
    duckdb.DuckDBPyRelation: the data from the file as a duckdb table
    """
    if data_format == "csv":
        return duckdb_conn.sql(f"select *,current_timestamp as LOAD_TS,current_timestamp as UPDATE_TS from read_csv('{file_path}',filename=True)")
    elif data_format == "xlsx":
        return duckdb_conn.sql(f"select *,'{file_path}' as filename,current_timestamp as LOAD_TS,current_timestamp as UPDATE_TS from read_xlsx('{file_path}',all_varchar=True)")


def get_fabric_metadata(property):
    std_property = property.lower()
    current_workspace = notebookutils.runtime.context['currentWorkspaceName']
    current_lakehouse = notebookutils.runtime.context['defaultLakehouseName']
    current_user = notebookutils.runtime.context['userName']
    current_notebook = notebookutils.runtime.context['currentNotebookName']
    ist_time = get_time()
    if std_property == "workspace":
        return current_workspace
    elif std_property == "lakehouse":
        return current_lakehouse
    elif std_property == "user":
        return current_user
    elif std_property == "notebook":
        return current_notebook
    elif std_property == "header":
        return f"Notebook - '{current_notebook}' execution started at {ist_time} by user - '{current_user}'"
    elif std_property == "footer":
        return f"Notebook - '{current_notebook}' execution completed at {ist_time}"

def get_delta_table_path(schema,table_name):
    current_lh = get_fabric_metadata('lakehouse')
    current_ws = get_fabric_metadata('workspace')
    table_path = f"abfss://{current_ws}@onelake.dfs.fabric.microsoft.com/{current_lh}.Lakehouse/Tables/{schema}/{table_name}"
    return table_path

def get_lakehouse_files(folder_path="/lakehouse/default/Files/", pattern=None):
    files = notebookutils.fs.ls(folder_path)
    file_list = []
    for file_info in files:
        if file_info.name.endswith((".csv", ".xlsx")):
            if pattern is None or pattern in file_info.name:
                file_list.append(file_info.path)
    return file_list
