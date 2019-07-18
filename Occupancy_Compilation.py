import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime

from SAP_DB_Filter import create_connection

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

# Single MEntity example ----
mentity_sample = np.array(['M0000000503', 'M0000000505'])

# SQL Connection ----
wss_engine = create_connection(database='Storage')
wss_query = 'SELECT * FROM [Storage].[dbo].[WSS_UnitMixUHI_Monthly_Archive] WHERE MEntity in {}'.format(tuple(mentity_sample))
wss_db = pd.read_sql_query(wss_query, wss_engine)
wss_engine.close()

