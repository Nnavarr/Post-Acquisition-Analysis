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

# import table
dates_table = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Power BI Dashboard\fiscal_dates.xlsx')

user = "1217543"

# Traditional SQL Connection protocol - this is still necessary
base_con = (
    "Driver={{ODBC DRIVER 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database=DEVTEST;"
    "UID={};"
    "PWD={};"
).format(user, os.environ.get("sql_pwd"))

# URLLib finds the important information from our base connection
params = urllib.parse.quote_plus(base_con)

# SQLAlchemy takes all this info to create the engine
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# upload to SQL
dates_table.to_sql('fiscal_dates', engine, index=False, if_exists='replace')