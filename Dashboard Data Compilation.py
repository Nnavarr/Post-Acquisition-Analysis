import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re


# ----------------
# Data Connections
# ----------------
user = '1217543'

# SQL Connection Function ----
def create_connection(database):
    #load password from env, entry if not available
    pwd = os.environ.get('sql_pwd')
    if pwd is None:
        pwd = getpass()

    #load user and create connection string
    cnxn_str = ((r'Driver={{SQL Server}};'
    r'Server=OPSReport02.uhaul.amerco.org;'
    r'Database='+database+';'
    r'UID={};PWD={};').format(user, pwd))

    #return connection object
    return(pyodbc.connect(cnxn_str))

# SAP Data Connection ----
sap_engine = create_connection(database='SAP_Data')

# SAP Account Numbers ----
sap_accounts = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\sap_accounts.csv')

# Entity List ----
entity_list = pd.read_excel(r'\\\\adfs01.uhi.amerco\\departments\\mia\\group\\MIA\\Noe\\Projects\\Post Acquisition\\Report\\Quarterly Acquisitions\\F19 Q4\\New Acquisitions List\\Master_Acquisitions_List.xlsx')









# SAP Account Numbers ----
sap_accounts = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\income_statement_accounts.csv')

# ----------------
# Data Aggregation
# ----------------
entity_list = pd.read_excel(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\F19 Q4\New Acquisitions List\Master_Acquisitions_List.xlsx')
entity_list['Profit_Center'] = entity_list['Profit_Center'].fillna(0).astype('int64')
entity_list['Profit_Center'] = entity_list['Profit_Center'].astype('object')

# Check for duplicate Profit Center Numbers within "Included" Rows ----
entity_list_include = entity_list[entity_list['Include?'] == 'Yes']
assert sum(entity_list_include.duplicated(subset='Profit_Center')) == 0, 'Duplicate Profit Centers Present, DO NO PROCEED'

# Query SQL DB ----
sap_engine = create_connection(database='SAP_Data')
sap_db_query = 'SELECT * FROM [SAP_Data].[dbo].[FAGLFLEXT] WHERE [PROFIT_CENTER] in {}'.format(tuple(entity_list_include['Profit_Center']))
sap_db = pd.read_sql_query(sap_db_query, sap_engine)
sap_engine.close()

# Reformat SAP Data ----
sap_db.rename(columns={'GC_PER_1': '1',
                       'GC_PER_2': '2',
                       'GC_PER_3': '3',
                       'GC_PER_4': '4',
                       'GC_PER_5': '5',
                       'GC_PER_6': '6',
                       'GC_PER_7': '7',
                       'GC_PER_8': '8',
                       'GC_PER_9': '9',
                       'GC_PER_10': '10',
                       'GC_PER_11': '11',
                       'GC_PER_12': '12'}, inplace=True)

# -------------------------
# Income Statement Function
# -------------------------
def income_statement(x):
    test_filter = sap_db[sap_db['PROFIT_CENTER'].isin(x['Profit_Center'])]
    return(test_filter)

income_statement(entity_list)

entity_list['Profit_Center']

