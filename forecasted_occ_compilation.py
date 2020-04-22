import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime
import sqlalchemy, urllib
from textwrap import dedent
from datetime import timedelta, date

"""
Creation: 4/21/2020
Author: Noe Navarro
SQL DB: Storage
Table: [FINANALYSIS].[dbo].[Storage_Forecast]

"""

def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user from env, user = 1217543 if not available
    user = os.environ.get("sql_user")
    if len(user) < 7:
        user = '1217543'

    # load user and create connection string
    cnxn_str = (
        r"Driver={{ODBC Driver 17 for SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)


"""

"""

# forecasted data import
engine = create_connection(database='FINANALYSIS')
query = dedent("""

    SELECT *
    FROM [FINANALYSIS].[dbo].[Storage_Forecast]

""")
forecast_df = pd.read_sql_query(query, engine)
engine.close()

# current date & wss import
cur_month = date.today().replace(day=1)
cur_month = cur_month - pd.DateOffset(months=1)
cur_month = datetime.date.strftime(cur_month, format="%Y-%m-%d")

engine = create_connection(database='Storage')
query = dedent("""

    SELECT *
    FROM [Storage].[dbo].[WSS_UnitMixUHI_Monthly_Archive]
    WHERE [MEntity] in {}
    AND [Date] = '{}'

""").format(tuple(forecast_df.MEntity.unique()), cur_month)
wss_df = pd.read_sql_query(query, engine)
