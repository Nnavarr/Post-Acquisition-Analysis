import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime
import sqlalchemy, urllib

"""
Creation: 2/27/2020
Author: Noe Navarro
SQL DB: Storage
Table: WSS_UnitMixUHI_Monthly_Archive

Objective: Occupancy metric compilation based on MEntity number. 
Methodology:
    1) Excludes U-Box
    
"""

user = '1217543'
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

def center_list_import():

    # quarter acquisitions process (SQL table pull)
    if aggregation_type == 'True':
        engine = create_connection(database='DEVTEST')
        center_list_query = "SELECT * " \
                            "FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List] " \
                            "WHERE [Include?] = 1"
        center_list = pd.read_sql_query(center_list_query, engine)
        engine.close()

    else:
        wd = os.getcwd()
        filename = input("What is the csv file name within the directory? \n Note: The file must be located within the same working directory. \n Also, please make sure there is a 'MEntity' column present. ")
        file_string = f"{wd}\{filename}"
        center_list = pd.read_csv(rf'{file_string}')

    return center_list

def occupancy_compilation(df):

    # establish connection
    engine = create_connection(database='Storage')
    wss_query = "SELECT * " \
                "FROM [Storage].[dbo].[WSS_UnitMixUHI_Monthly_Archive] " \
                "WHERE [MEntity] in {} " \
                "AND [unm_product] != 'UBOX'".format(tuple(df.MEntity.unique()))

    wss_df = pd.read_sql_query(wss_query, engine)

    # aggreate by MEntity
    wss_agg = wss_df.groupby(['Date', 'MEntity', 'unm_product'])
    occ_df = wss_agg[['unm_numunits', 'unm_occunits']].sum().sort_values('Date')
    engine.close()

    # format df
    occ_df = occ_df.reset_index()
    col_type_update = ['unm_numunits', 'unm_occunits']
    for col in col_type_update:
        occ_df[col] = occ_df[col].astype(int)

    return occ_df

def data_export(df):

    if aggregation_type == 'True':

        try:
            # upload to SQL ----
            base_con = (
                "Driver={{ODBC DRIVER 17 for SQL Server}};"
                "Server=OPSReport02.uhaul.amerco.org;"
                "Database=DEVTEST;"
                "UID={};"
                "PWD={};"
            ).format(user, os.environ.get("sql_pwd"))

            params = urllib.parse.quote_plus(base_con)
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df.to_sql('Quarterly_Acquisitions_Occ', engine, index=False, if_exists='append')
            engine.close()
            print("The quarterly acquisitions occupancy was uploaded successfully")
        except:
            print("An erorr occurred with the occupancy aggregation and upload ")
    else:
        try:
            filepath = input("What is the full file path for the csv output file? ")
            df.to_csv(rf'{filepath}', index=False)
            print("The file has been saved successfully")
        except:
            print("An error occurred with the occupancy aggregation")

    return print("The process is now shutting down.")

if __name__ == '__main__':

    aggregation_type = input("Is this the quarterly acquisitions aggregation? (True or False)")
    center_list = center_list_import()
    aggregation = occupancy_compilation(center_list)
    export = data_export(aggregation)









