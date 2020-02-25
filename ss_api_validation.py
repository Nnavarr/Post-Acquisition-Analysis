import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

# Create connection ----
user = "1217543"
# SQL Connection -----
def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user and create connection string
    cnxn_str = (
        r"Driver={{SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)

"""
Smartsheet API vs Real Additions Inclusion: Missing Real Additions from API
    Objective: Determine discrepancies between smartsheet API and real additions DB'
    This could provide guidance in who we need to contact in order to obtain a better set of information
"""
# establish connection & import data ----
real_add_engine = create_connection(database='RealEstateValuation')
smartsheet_query = "SELECT * " \
                   "FROM [RealEstateValuation].[dbo].[Smartsheet_Closed]"
smartsheet_df = pd.read_sql_query(smartsheet_query, real_add_engine)
real_add_query = "SELECT * " \
                 "FROM [RealEstateValuation].[dbo].[REV_REAL_ADDITIONS]"
real_add_df = pd.read_sql_query(real_add_query, real_add_engine)
real_add_engine.close()

"""
Initial EDA 2/11/2020

    Smartsheet
    ----------
        Rows = 744
        Unique MEntity = 679
        Missing MEntity = 0
        Min_Date = 2014-04-23
    
    Real Additions 
    --------------
    Rows = 872
    Unique MEntity = 833
    Missing MEntity = 4
    Min_Date [Closing_Date] = 1994-03-25
    
        Due to the discrepancy in minimum dates, we will want to check how many observations are present within
        both sets of data as measured by the MEntity number (our unique identifier).
    
"""

# Inner Join on both DFs: Real Additions will be our starting point table ----
join_real_add = pd.merge(left=real_add_df, right=smartsheet_df, how='inner', on='MEntity',
                         suffixes=['_real_add', '_smartsheet'])

"""
Inner_join results 2/11/2020:
    
    Rows: 683
    Unique MEntity: 587 
    
    Duplication: 96 duplicates were introduced when creating the inner join on the real additions DB. 
    Match %: 86.5% of MEntity numbers from the Smart-Sheet API were matched to the real additions.
    
    Conclusion: 
        We can conclude the smartsheet API appears to have more in terms of MEntity numbers when compared to the
        real additions tool. However, 86% is not a bad percent.
                 
"""

