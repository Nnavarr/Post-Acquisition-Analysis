import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
from sap_db_filter import create_connection
import re

# SQL Alchemy ----
import sqlalchemy, urllib

# user
user = '1217543'

# read center list 2/24/2020
center_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\center_list\acquisitions_list_final.xlsx',
                            sheet_name='Sheet1')
columns_to_bool = ['Include?','Abutting', 'Remote']
for name in columns_to_bool:
    center_list[name] = center_list[name].apply(lambda x: False if x == 'No' else True)

# establish connection ----
base_con = (
    "Driver={{ODBC DRIVER 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database=DEVTEST;"
    "UID={};"
    "PWD={};"
).format(user, os.environ.get("sql_pwd"))
con = pyodbc.connect(base_con)
params = urllib.parse.quote_plus(base_con)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# upload to devtest ----
center_list.to_sql('Quarterly_Acquisitions_List',
                   engine,
                   index=False,
                   if_exists='replace')
