import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

user = '1217543'

# ------------------
# Occupancy Function
# ------------------

"""
Creation: 7/16/2019
Author: Noe Navarro
SQL DB: Storage
Table: WSS_UnitMixUHI_Monthly_Archive

Occupancy function will be based off MEntity numbers. The extraction of these MEntity numbers will need to be completed
prior to running the occupancy function. 

"""

# Open Connection to Occupancy Data ----
sap_db_query = 'SELECT * FROM [SAP_Data].[dbo].[FAGLFLEXT] WHERE [PROFIT_CENTER] in {} AND [GL_ACCOUNT] in {}'.format(tuple(entity_in['Profit_Center']), tuple(sap_accounts['Account']))
sap_db = pd.read_sql_query(sap_db_query, sap_engine)
sap_engine.close()

single_mentity = 'M0000117667'

# Create Occupancy Function ----
