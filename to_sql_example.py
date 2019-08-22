#Packages for organized data
import pandas as pd
import numpy as np
#General packages - helps managing user credentials
import os
from getpass import getuser, getpass
#Traditional SQL connection via ODBC protocol
import pyodbc
#Packages necessary for pd.to_sql() functionality 
import sqlalchemy, urllib

#Generate a dataframe
data = pd.DataFrame(
    np.random.randn(100, 4), columns = ['A', 'B', 'C', 'D']
)

user = '1217543'

#Traditional SQL Connection protocol - this is still necessary
base_con = (
    'Driver={{ODBC DRIVER 17 for SQL Server}};'
    'Server=OPSReport02.uhaul.amerco.org;'
    'Database=DEVTEST;'
    'UID={};'
    'PWD={};'
).format(user, os.environ.get("sql_pwd"))
con=pyodbc.connect(base_con)

#URLLib finds the important information from our base connection
params = urllib.parse.quote_plus(base_con)
#SQLAlchemy takes all this info to create the engine
engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

#The first argument is the table name
#The second argument will always be the SQLAlchemy engine
#if_exists dictates what will happen if the table name already exists
#pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
data.to_sql(test_table, engine, if_exists='replace')


