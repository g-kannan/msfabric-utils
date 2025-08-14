import duckdb
from deltalake import DeltaTable,write_deltalake
from datetime import datetime
import pytz
from loguru import logger
import uuid
import notebookutils

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

def read_lakehouse_files(duckdb_conn,data_format,file_path):
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
    """
    Returns a list of files in the specified folder path with the specified pattern.
    
    Parameters:
    folder_path (str): the path to the folder to list files from
    pattern (str): the pattern to match against file names
    
    Returns:
    list: a list of file paths that match the pattern
    """
    files = notebookutils.fs.ls(folder_path)
    file_list = []
    for file_info in files:
        if file_info.name.lower().endswith((".csv", ".xlsx")):
            if pattern is None or pattern in file_info.name:
                file_list.append(file_info.path)
    return file_list

def write_delta_table(arrow_df,schema,table_name,mode="append"):
    """
    Writes data to a delta table in the specified schema and table name.
    
    Parameters:
    arrow_df (pyarrow.Table): the data to write to the delta table
    schema (str): the schema of the delta table
    table_name (str): the name of the delta table
    mode (str): the mode to use when writing the data, either 'append' or 'overwrite'
    
    Returns:
    str: a message indicating the result of the write operation
    """
    table_path = get_delta_table_path(schema,table_name)
    storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
    logger.info(f"Write Mode: {mode}, Using table path:{table_path}")
    try:
        write_deltalake(table_path, arrow_df, mode=mode, engine='rust', storage_options=storage_options)
    except Exception as e:
        logger.exception(
            f"Unexpected error appending data to Delta table: {e}",
            extra={"path": table_path, "schema": schema, "table": table_name}
        )
        return f"Error '{e}' occured during write"

def create_view_lakehouse_files(duckdb_conn,data_format,file_path,all_varchar=False,normalize_names=False,sheet=None):
    """
    Creates views for a lakehouse file with summary
    
    Parameters:
    duckdb_conn (duckdb.Connection): an open connection to a duckdb database
    data_format (str): the format of the file, either 'csv' or 'xlsx'
    file_path (str): the path to the file to create views for
    all_varchar (bool): whether to read the file as all varchar
    normalize_names (bool): whether to normalize column names
    
    Returns:
    str: a message indicating the result of the view creation
    """
    if data_format == "csv":
        duckdb_conn.sql(f"create or replace view vw_csv as select *,current_timestamp as LOAD_TS,current_timestamp as UPDATE_TS from read_csv('{file_path}',filename=True,all_varchar={all_varchar},normalize_names={normalize_names})")
        duckdb_conn.sql("create or replace view vw_csv_summary as SELECT * FROM (SUMMARIZE vw_csv);")
        return "View vw_csv & vw_csv_summary created"
    elif data_format == "xlsx":
        if sheet:
            duckdb_conn.sql(f"create or replace view vw_xlsx as select *,'{file_path}' as filename,current_timestamp as LOAD_TS,current_timestamp as UPDATE_TS from read_xlsx('{file_path}',all_varchar={all_varchar},sheet={sheet})")
        else:
            duckdb_conn.sql(f"create or replace view vw_xlsx as select *,'{file_path}' as filename,current_timestamp as LOAD_TS,current_timestamp as UPDATE_TS from read_xlsx('{file_path}',all_varchar={all_varchar})")
        duckdb_conn.sql("create or replace view vw_xlsx_summary as SELECT * FROM (SUMMARIZE vw_xlsx);")
        return "View vw_xlsx & vw_xlsx_summary created"