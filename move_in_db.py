import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

# SQL
user = '1217543'

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

engine = create_connection(database='DEVTEST')
movein_query = "SELECT * FROM [DEVTEST].[dbo].[March2020MoveInMoveOut]"
df = pd.read_sql_query(movein_query, engine)
engine.close()

# EDA ----
# duplicate_mask = df.con_pk.value_counts() > 1
# duplicate_df = pd.DataFrame(duplicate_df).reset_index()
# duplicate_df.to_csv(r'V:\STORAGEGROUP\Jacob\7. Noe\Round 2\duplicate_con_pk.csv')


# concat
df['unique_id'] = df.con_entity + df.cus_firstname + df.cus_lastname


 """ Potential issues """

# mask for same moveout ----
df['same_day'] = np.where(df.det_movein==df.det_enddate, True, False)
sum(df.same_day == True) / df.shape[0] # 0.0686327982961924 same day move in and out


# remove duplicate con_pk ----
df_unique_conpk = df.drop_duplicates(subset='con_pk')

# Test for duplicates ----
march 8th - march 17

cy_start = pd.to_datetime('2020-03-08')
cy_end = pd.to_datetime('2020-03-17')

# filter
cy_mask = (df.det_movein >= cy_start) & (df.det_movein <= cy_end)
cy_df = df[cy_mask]

# last year
ly_start = pd.to_datetime('2019-03-08')
ly_end = pd.to_datetime('2019-03-17')

ly_mask = (df.det_movein >= ly_start) & (df.det_movein <= ly_end)
ly_df = df[ly_mask]

# comparison
cy_df.shape
ly_df.shape


# unique det_pk ----
cy_unique_conpk = cy_df.drop_duplicates(subset='con_pk') #21,106 unique con_pk
ubox_filter = cy_unique_conpk.esc_product != 'Ubox' # true if keep
cy_final = cy_unique_conpk[ubox_filter]
cy_final.to_csv(r'V:\STORAGEGROUP\Jacob\7. Noe\Round 2\cy_final.csv')


""" WSS MOVE IN MOVE OUT"""
engine = create_connection(database='Storage')
wss_query = "SELECT * FROM [Storage].[dbo].[WSS_MoveInMoveOut] " \
            "WHERE [det_movein] >= '2019-03-08' AND [det_movein] <= '2019-03-19' " \
            "AND [esc_product] <> 'UBOX' "

wss_df = pd.read_sql_query(wss_query, engine)
engine.close()

# unique con_pk
wss_final_ly = wss_df.drop_duplicates(subset='con_pk') #15,583 unique con_pk
wss_final_ly.to_csv(r'V:\STORAGEGROUP\Jacob\7. Noe\Round 2\ly_final.csv')
wss_df.det_movein.value_counts()
