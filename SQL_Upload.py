import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

# Packages necessary for pd.to_sql() functionality
import sqlalchemy, urllib

# Income Statement Function Import
from Dashboard_Data_Compilation import income_statement, create_connection, sap_db, chart_of_accounts

user = '1217543'

#Traditional SQL Connection protocol - this is still necessary
base_con = (
    'Driver={{ODBC DRIVER 17 for SQL Server}};'
    'Server=OPSReport02.uhaul.amerco.org;'
    'Database=DEVTEST;'
    'UID={};'
    'PWD={};'
).format(user, os.environ.get("sql_pwd"))
con = pyodbc.connect(base_con)

#URLLib finds the important information from our base connection
params = urllib.parse.quote_plus(base_con)

#SQLAlchemy takes all this info to create the engine
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
connection = engine.raw_connection()

# ---------------------------------------------------------------------------------------------------------------------

# ----------------------------------
# All Center Income Statement Upload
# ----------------------------------

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

graph_entity_list.rename(columns={'Cost Center': 'Profit Center'}, inplace=True)
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
# exploration = graph_entity_list.groupby(['Simple Owner']).agg(['count'])
# exploration['Profit Center']

# Extract UHI Centers ----
arec_mask = graph_entity_list['Simple Owner'] == 'UHI'
arec_entity = graph_entity_list[arec_mask]
arec_pc = arec_entity['Profit Center'].unique()

arec_pc = arec_pc[arec_pc != None]
arec_pc = arec_pc[arec_pc != '0']

# Compile Individual Income Statement DF ----
arec_pc_list = list(arec_pc)

is_container = map(lambda x: income_statement(profit_center=x,
                                  sap_data=sap_db,
                                  line_item_dict=chart_of_accounts), arec_pc_list)

arec_income_statement_list = [n for n in list(is_container)]

# Dictionary Containing all Income Statements ----
income_statement_dict = dict(zip(arec_pc_list, arec_income_statement_list))

# Concatenate into a single data frame for SQL upload ----
aggregate_df = pd.concat(income_statement_dict)

# Upload To SQL ----
aggregate_df.to_sql('Center_IS', engine, index=False, if_exists='replace')
#pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

