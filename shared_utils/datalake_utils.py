import duckdb
from deltalake import DeltaTable,write_deltalake
from datetime import datetime,timedelta
import pytz
from loguru import logger
import uuid
import notebookutils
import polars as pl

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
        write_deltalake(table_path, arrow_df, mode=mode, storage_options=storage_options)
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
        duckdb_conn.sql(f"create or replace view vw_csv as select * from read_csv('{file_path}',header=True,filename=True,all_varchar={all_varchar},normalize_names={normalize_names})")
        duckdb_conn.sql("create or replace view vw_csv_summary as SELECT * FROM (SUMMARIZE vw_csv);")
        return "View vw_csv & vw_csv_summary created"
    elif data_format == "xlsx":
        if sheet:
            duckdb_conn.sql(f"create or replace view vw_xlsx as select *,'{file_path}' as filename from read_xlsx('{file_path}',header=True,all_varchar={all_varchar},sheet='{sheet}')")
        else:
            duckdb_conn.sql(f"create or replace view vw_xlsx as select *,'{file_path}' as filename from read_xlsx('{file_path}',header=True,all_varchar={all_varchar})")
        duckdb_conn.sql("create or replace view vw_xlsx_summary as SELECT * FROM (SUMMARIZE vw_xlsx);")
        return "View vw_xlsx & vw_xlsx_summary created"


def get_lakehouse_table_path(schema,table_name):
    """
    Returns the path to a lakehouse table in the default lakehouse.
    
    Parameters:
    schema (str): the schema of the table
    table_name (str): the name of the table
    
    Returns:
    str: the path to the table
    """
    table_path = f"/lakehouse/default/Tables/{schema}/{table_name}"
    return table_path

def get_ist_datetime(format_type=None):
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)

    if format_type == "iso":
        return now_ist.isoformat()
    elif format_type == "date":
        return now_ist.strftime('%Y-%m-%d')
    elif format_type == "time":
        return now_ist.strftime('%H:%M:%S')
    else:  # default
        return now_ist.strftime('%Y-%m-%dT%H:%M:%S')


def run_notebook(job_config, params):
    """Run a notebook safely and return exit_value + outcome."""
    notebook_to_run = job_config["NOTEBOOK"]
    logger.info(f"Executing notebook: - {notebook_to_run}")
    try:
        exit_value = notebookutils.notebook.run(notebook_to_run, 300, params)
        outcome = "SUCCESS"
    except Exception as e:
        exit_value = str(e)  # capture error details
        outcome = "FAILURE"
    return exit_value, outcome

def audit_run(job_config, file_to_process,context, exit_value, outcome):
    """Append pipeline audit table with pipeline + runtime metadata."""
    # runtime context values
    isRunFromPipeline = notebookutils.runtime.context['isForPipeline']
    executed_by = notebookutils.runtime.context['userName']

    # select required config
    selected_config = {k: job_config[k] for k in ["DATA_SOURCE", "NOTEBOOK", "TARGET_SCHEMA", "TARGET_OBJECT", "WRITE_MODE"]}

    # build audit dataframe
    df = pl.DataFrame({
        "RUN_ID": [context["run_id"]],
        "TRIGGER_TIME": [context["trigger_time"]],
        "PIPELINE_NAME": [context["pipeline_name"]],
        "TRIGGER_TYPE": [context["trigger_type"]],
        "IS_RUN_FROM_PIPELINE": [isRunFromPipeline],
        "EXECUTED_BY": [executed_by],
        "DATA_SOURCE": [selected_config["DATA_SOURCE"]],
        "NOTEBOOK": [selected_config["NOTEBOOK"]],
        "TARGET_SCHEMA": [selected_config["TARGET_SCHEMA"]],
        "TARGET_OBJECT": [selected_config["TARGET_OBJECT"]],
        "WRITE_MODE": [selected_config["WRITE_MODE"]],
        "INPUT_FILE": [file_to_process],
        "EXIT_MESSAGE": [exit_value],
        "OUTCOME": [outcome]
    })

    write_delta_table(df,"AUDIT","PIPELINE_RUNS",mode="append")

def move_trailing_minus(value: str) -> str:
    """
    If the string ends with '-', move it to the front.
    Example: '286.64-' -> '-286.64'
    """
    if value is None:
        return None
    value = str(value).strip()
    if value.endswith('-'):
        return '-' + value[:-1]
    return value

def move_lakehouse_file(src_path: str, dest_dir: str, create_path: bool = False, overwrite: bool = False):
    """
    Move a file from src_path to dest_dir using notebookutils.fs.mv.
    Extracts the filename from src_path.

    Args:
        src_path (str): Full source path of the file
        dest_dir (str): Destination directory path (without filename, must end with '/')
        create_path (bool): Whether to create the destination path if it does not exist
        overwrite (bool): Whether to overwrite the file if it exists at destination
    """
    try:
        # Extract filename from source path
        filename = src_path.split("/")[-1]

        # Ensure destination dir ends with '/'
        if not dest_dir.endswith("/"):
            dest_dir += "/"

        dest_path = dest_dir + filename

        notebookutils.fs.mv(src_path, dest_path, create_path=create_path, overwrite=overwrite)
        print(f"File moved successfully:\n   {src_path} -> {dest_path}")
    except Exception as e:
        print(f"Failed to move file:\n   {src_path} -> {dest_dir}\nError: {str(e)}")

def excel_serial_to_date(n) -> str:
    if n is None:
        return None
    try:
        n = int(n)   # force string → int if needed
        return (datetime(1899, 12, 30) + timedelta(days=n)).date()
    except Exception:
        return None    