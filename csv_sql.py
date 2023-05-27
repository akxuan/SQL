"""helper functions to ingest required csv's and setup functions for sql"""

def pd_to_sql(input_df: pd.DataFrame
              , table_name: str
              , db_name: str = 'raw.db') -> None:
    """Take a Pandas dataframe `input_df` and upload it to `table_name` SQLITE table.
    
    Args:
        input_df (pd.DataFrame): Dataframe containing data to upload to sql db.
        table_name (str): Name of the table to upload to.
        db_name (str, optional): Name of the database to create table. 
                                 Defaults to 'raw'.
                                 
    """

    # Find columns in the dataframe
    cols = input_df.columns
    cols_string = ','.join(cols)
    val_wildcard_string = ','.join(['?'] * len(cols))

    # Connect to a DB file if it exists, else crete a new file
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # drop table if exists, create new table
    sql_string_drop = f"""DROP TABLE IF EXISTS {table_name};"""
    cur.execute(sql_string_drop)
    sql_string_create = f"""CREATE TABLE {table_name} ({cols_string});"""
    cur.execute(sql_string_create)

    # write df to table
    rows_to_upload = input_df.to_dict(orient='split')['data']
    sql_string = f"""INSERT INTO {table_name} ({cols_string}) VALUES ({val_wildcard_string});"""    
    cur.executemany(sql_string, rows_to_upload)
  
    # commit the changes and close the connection
    con.commit()
    con.close()


def sql_to_pd(sql_query_string: str
              , db_name: str ='raw.db') -> pd.DataFrame:
    """Execute a SQL query and return the results as a pandas dataframe.

    Args:
        sql_query_string (str): SQL query string to execute.
        db_name (str, optional): Name of the SQLITE Database to execute the query in.
                                 Defaults to 'raw'.
    Returns:
        pd.DataFrame: Results of the SQL query in a pandas dataframe.
    
    """

    # connect to the SQL DB and execute query
    con = sqlite3.connect(db_name)
    cursor = con.execute(sql_query_string)

    # fetch values and column names
    result_data = cursor.fetchall()
    cols = [description[0] for description in cursor.description]
    con.close()

    return pd.DataFrame(result_data, columns=cols)


files = {
      "https://storage.googleapis.com/covid19-open-data/v3/index.csv": "idx",
      "https://storage.googleapis.com/covid19-open-data/v3/demographics.csv": "demographics",
      "https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv": "hospitalizations"
}

def read_files_to_sql(file_dict: dict):
  """Method to ingest csv files to sql from various locations.
  
  Depending on file source, will either read url, or read github.

  Args:
    file_dict (dict): file_dict, dictionary of files to read, and aliases to save table as.

  """

for k,v in files.items():
  input_df = pd.read_csv(k)
  pd_to_sql(input_df, v)
