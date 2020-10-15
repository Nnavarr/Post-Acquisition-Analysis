import numpy as np
import pandas as pd
import pyodbc
import os
from getpass import getpass
import sqlalchemy, urllib

# import manual compilation from Excel
center_df = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\2020\Post Acquisition\Quarterly Acquisitions\F21 Q1\excel_acq_list_upload.xlsx')

# SQL alchemy connection for upload
user = '1217543'

base_con = (
    "Driver={{ODBC DRIVER 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database=DEVTEST;"
    "UID={};"
    "PWD={};"
).format(user, os.environ.get("sql_pwd"))

params = urllib.parse.quote_plus(base_con)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

"""
Important (4/27/2020): This was created as a one time upload due to smartsheet api not working properly. The code
to upload has been removed from the script to ensure no accidents occur.
"""

# Upload new centers to existing SQL list
#center_df.to_sql('Quarterly_Acquisitions_List', engine, if_exists='append', index=False)
