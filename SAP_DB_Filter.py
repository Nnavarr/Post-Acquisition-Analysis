import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

# -----------------------
# SQL Connection Function
# -----------------------
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


# -----------------------------
# Data Processing: SAP_Accounts
# -----------------------------
# SAP Chart of Accounts ----
sap_accounts = pd.read_csv(
    r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\sap_accounts.csv')

sap_accounts.rename(columns={'SAP ACC. Number': 'Account',
                             'Lender Trend Line Item': 'line_item'}, inplace=True)
sap_accounts['Account'] = sap_accounts['Account'].astype('object')

# Retain relevant accounts ----
"""
SAP accounts can be added or removed within this next section. Accounts have been known to be added without notice from the accounting department
"""

included_accounts_mask = sap_accounts['line_item'] != 'NOT USED FOR LENDER REPORTING'
sap_accounts = sap_accounts[included_accounts_mask]

# Income Statement Line Item Creation ----
line_items = sap_accounts['line_item'].unique()

# Test DF Container ----
separate_df_container = []

# Create Individual Dataframes of line items ----
for category in line_items:
    account_mask = sap_accounts['line_item'] == category
    separate_df = sap_accounts[account_mask]
    separate_df_container.append(separate_df)

# Create Dictionary of Income Statement Line Items ----
chart_of_accounts = dict(zip(line_items, separate_df_container))

# Checkpoint: Chart of Accounts Complete
# ---------------------------------------


# -------------
# SAP DB Query
# -------------
# SAP Data Connection ----
sap_engine = create_connection(database='SAP_Data')

def sap_db_query(profit_center_list):

    sap_db_query = 'SELECT * FROM [SAP_Data].[dbo].[FAGLFLEXT] WHERE [PROFIT_CENTER] in {} AND [GL_ACCOUNT] in {}'.format(tuple(profit_center_list), tuple(sap_accounts['Account']))
    sap_db = pd.read_sql_query(sap_db_query, sap_engine)
    sap_engine.close()

    sap_db = sap_db.select(lambda x: not re.search('LC\w', x), axis=1)
    sap_db.rename(columns={'GC_PER_1': '04',
                           'GC_PER_2': '05',
                           'GC_PER_3': '06',
                           'GC_PER_4': '07',
                           'GC_PER_5': '08',
                           'GC_PER_6': '09',
                           'GC_PER_7': '10',
                           'GC_PER_8': '11',
                           'GC_PER_9': '12',
                           'GC_PER_10': '01',
                           'GC_PER_11': '02',
                           'GC_PER_12': '03',
                           'PROFIT_CENTER': 'Profit_Center',
                           'GL_ACCOUNT': 'Account',
                           'FISCAL_YEAR': 'Fiscal_Year'}, inplace=True)

    # Date column creation ----
    sap_db = sap_db.melt(id_vars=['Fiscal_Year', 'Profit_Center', 'Account','CO_AREA', 'TRANS_CURR', 'COMPANY_CODE', 'SEGMENT', 'COST_CENTER'])
    sap_db.rename(columns={'variable': 'month'}, inplace=True)

    # Create a function to generate Date Column based on
    sap_db['month'] = sap_db['month'].astype('int64')
    sap_db['Fiscal_Year'] = sap_db['Fiscal_Year'].astype('int64')
    sap_db['year'] = np.where((sap_db['month'] >= 4) & (sap_db['month'] <= 12), sap_db['Fiscal_Year'] - 1, sap_db['Fiscal_Year'])
    sap_db['month'] = sap_db['month'].astype('object')
    sap_db['year'] = sap_db['year'].astype('object')
    sap_db['Date'] = sap_db['year'].map(str) + '-' + sap_db['month'].map(str) + '-01'
    sap_db['Date'] = pd.to_datetime(sap_db['Date'], format='%Y-%m-%d')

    return(sap_db)

# Test SAP DB Function ----
# Profit Center List ----
entity_info_con = create_connection(database='FINANALYSIS')
graph_index_query = "SELECT * FROM [FINANALYSIS].[dbo].[GRAPH_INDEX_MATCH]"
graph_entity_info_query = "SELECT * FROM [FINANALYSIS].[dbo].[GRAPH_ENTITY_INFO]"

# Import Graph Entity List ----
index_match_db = pd.read_sql_query(graph_index_query, entity_info_con)
entity_info_db = pd.read_sql_query(graph_entity_info_query, entity_info_con)
entity_info_con.close()

# Import Owner to Entity Info ----
graph_entity_list = pd.merge(left=entity_info_db, right=index_match_db.loc[:, ['MEntity', 'Simple Owner', 'SAC or Galaxy or PMSR',
                                                                               'Owned']],
                             how='left', on='MEntity')

graph_entity_list.rename(columns={'Cost Center': 'Profit_Center'}, inplace=True)
"""
Unique "Simple Owner" within the graph file includes:

1) UHI: 1577
2) SAC: 425
3) Mercury: 78
4) Galaxy: 16
5) DLR: 1
6) Closed: 34
7) None

For AREC, we are interested in UHI Centers
"""

# Extract UHI Centers ----
arec_mask = graph_entity_list['Simple Owner'] == 'UHI'
arec_entity = graph_entity_list[arec_mask]
arec_pc = arec_entity['Profit_Center'].unique()
arec_pc = arec_pc[arec_pc != None]
arec_pc = arec_pc[arec_pc != '0']
arec_pc_list = list(arec_pc)

# Example Use of Profit Center list with SAP_DB_Query Function ----
test_function = sap_db_query(arec_pc_list)
