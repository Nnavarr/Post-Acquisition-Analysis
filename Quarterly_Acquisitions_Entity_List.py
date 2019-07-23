import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime as dt

# Packages necessary for pd.to_sql() functionality
import sqlalchemy, urllib

from SAP_DB_Filter import create_connection

# -------------------------------------------
# Create SQL Connection & Download Table Data
# -------------------------------------------
user = '1217543'
devtest_con = create_connection(database='DEVTEST')

list_query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]"
master_list = pd.read_sql(list_query, devtest_con)
devtest_con.close()

# base_con = (
#     'Driver={{ODBC DRIVER 17 for SQL Server}};'
#     'Server=OPSReport02.uhaul.amerco.org;'
#     'Database=DEVTEST;'
#     'UID={};'
#     'PWD={};'
# ).format(user, os.environ.get("sql_pwd"))
# con = pyodbc.connect(base_con)
#
# #URLLib finds the important information from our base connection
# params = urllib.parse.quote_plus(base_con)
#
# #SQLAlchemy takes all this info to create the engine
# engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

# master_list.to_sql('Quarterly_Acquisitions_List', engine, index=False, if_exists='append')

# Import New Acquisitions List ----
min_date = pd.to_datetime('2019-04-01', format='%Y-%m-%d')
max_date = pd.to_datetime('2019-07-01', format='%Y-%m-%d')

new_list = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Closed Acquisitions FY 20.csv')

# Process imported data frame to match "existing_list" format ----
new_list['Close of Escrow'] = pd.to_datetime(new_list['Close of Escrow'], format='%m/%d/%y')
new_list = new_list.sort_values(by=['Close of Escrow'])
new_list.rename(columns={'Permanent Entity #': 'Entity'}, inplace=True)
new_list['Entity'] = new_list.Entity.astype(str)

# Apply date filter (if necessary) ----
min_date_mask = new_list['Close of Escrow'] >= min_date
max_date_mask = new_list['Close of Escrow'] < max_date

new_list = new_list[min_date_mask & max_date_mask]

# Check for Duplicate Entity values ----
assert max(new_list.Entity.value_counts()) == 1, 'Duplicate Entity within new_list DF'

# Create group columns ----
grp_num = 18
grp_fy = 2020
grp_quarter = 1

new_list.insert(0, "Group", grp_num)
new_list.insert(1, "FY", grp_fy)
new_list.insert(2, "Quarter", grp_quarter)

# Unique Entity numbers ----
unique_entity = new_list['Entity'].unique()

# Checkpoint: List ready for additional Entity info import ----

# -------------
# DLR01 Import
# -------------
mentity_engine = create_connection(database='MEntity')

dlr01_query_new = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} AND [STATUS] = 'O' ORDER BY [ENTITY_6NO] ASC".format(tuple(new_list.Entity))
dlr01_new_acquisitions = pd.read_sql_query(dlr01_query_new, mentity_engine)
dlr01_new_acquisitions.rename(columns={'ENTITY_6NO': 'Entity'}, inplace=True)

# New addition entity numbers ----
included_entity = np.isin(unique_entity, dlr01_new_acquisitions['Entity'])
assert included_entity.size == unique_entity.size, 'A new additions entity is not present within DLR01'

mentity_engine.close()

# -