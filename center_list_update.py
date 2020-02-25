import numpy as np
import pandas as pd
import pyodbc
import os
from getpass import getpass

user = '1217543'

# group dictionary ----
grp_dict = {'2020-3': 20,
            '2020-4': 21,
            '2021-1': 22,
            '2021-2': 23,
            '2021-3': 24,
            '2021-4': 25,
            '2022-1': 26,
            '2022-2': 27,
            '2022-3': 28,
            '2022-4': 29}

# establish slq connection ----
def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user and create connection string
    cnxn_str = (
        r"Driver={{ODBC Driver 17 for SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)

# connect to existing smartsheet api ----
def smart_sheet_import():

    # establish connection
    engine = create_connection(database='RealEstateValuation')
    query = "SELECT * FROM [RealEstateValuation].[dbo].[Smartsheet_Closed]"
    df = pd.read_sql_query(query, engine)
    df.sort_values('Close of Escrow', inplace=True)

    # update appropriate columns to datetime
    cols = ['Close of Escrow', 'Extract_Date']
    for name in cols:
        df[name] = pd.to_datetime(df[name], format='%Y-%m-%d')

    # drop index column
    df.drop('index', axis=1, inplace=True)
    engine.close()

    return df

# connect to existing list ----
def quarter_acq_import():

    # establish connection
    engine = create_connection(database='DEVTEST')
    query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]"
    df = pd.read_sql_query(query, engine)
    df.sort_values('Close of Escrow', inplace=True)
    engine.close()

    return df

# extract new observations (based on close of escrow) ----
def new_acquisitions():

    # import current lists
    smartsheet = smart_sheet_import()
    q_acquisitions = quarter_acq_import()

    # establish new center list
    max_q_acq_date = q_acquisitions['Close of Escrow'].max()
    date_mask = smartsheet['Close of Escrow'] > max_q_acq_date
    new_acq = smartsheet[date_mask].copy()

    # group classification
    new_acq['acq_grp'] = new_acq['FY Closed'].astype(str) + '-' + new_acq['Quarter Closed'].astype(str)
    new_acq['Group'] = new_acq.acq_grp.map(grp_dict)
    new_acq.drop('acq_grp', axis=1, inplace=True)

    return new_acq

# import relevant lists ----
new_list, q_acquisitions = new_acquisitions(), quarter_acq_import()

# column name requirements ----
acquisiton_columns = q_acquisitions.columns.values
new_list_columns = new_list.columns.values
present_cols = np.isin(new_list_columns, acquisiton_columns)

new_list_columns[~present_cols]

# extract current columns to csv for easier comparison of inclusion ----
new_list.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q4\new_list.csv',
                index=False)

q_acquisitions.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q4\acq_list.csv',
                      index=False)

