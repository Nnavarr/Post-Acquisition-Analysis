import numpy as np
import pandas as pd
import pyodbc
import os
from getpass import getpass
import sqlalchemy, urllib

user = '1217543'

def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

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
DLR01 Update
"""
def dlr01_source_update():

    conn = create_connection(database='DEVTEST')
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE 
        q_acq
    SET 
        ENTITY_NAME = dlr01.ENTITY_NAME,
        LOC_LATITUDE = dlr01.LOC_LATITUDE,
        LOC_LONGITUDE = dlr01.LOC_LONGITUDE
    
    FROM 
        [DEVTEST].[dbo].[Quarterly_Acquisitions_List] q_acq
        LEFT JOIN [MEntity].[dbo].[ENTITY_DLR01] dlr01
        ON q_acq.MEntity = dlr01.MEntity
        WHERE q_acq.MEntity = dlr01.MEntity
        AND dlr01.STATUS = 'O' 
    ''')
    conn.commit()
    conn.close()
    print("DLR01 updated successfully.")

"""
Graph Update
"""
def graph_source_update():

    conn = create_connection(database='DEVTEST')
    cursor = conn.cursor()

    # Index Match update ----
    cursor.execute('''
    
    UPDATE 
        q_acq
    SET
        [Simple Owner] = graph.[Simple Owner]
    FROM 
        [DEVTEST].[dbo].[Quarterly_Acquisitions_List] q_acq
        LEFT JOIN [Graph].[dbo].[Index Match] graph
        ON q_acq.MEntity = graph.MEntity
        WHERE q_acq.MEntity = graph.MEntity
    ''')

    cursor.commit()
    print("Index Match update complete...beginning Entity_Info")

    # Entity info update ----
    cursor.execute('''
    
    UPDATE
        q_acq
    SET
        [CBSA] = graph.[CBSA],
        [Profit_Center] = graph.[Cost Center]
    FROM 
        [DEVTEST].[dbo].[Quarterly_Acquisitions_List] q_acq
        LEFT JOIN [Graph].[dbo].[Entity Info] graph
        ON q_acq.MEntity = graph.MEntity
        WHERE q_acq.MEntity = graph.MEntity
    ''')

    cursor.commit()
    conn.close()
    print("Entity info update complete. All Graph data updates ran successfully")

"""
Real Additions Update
"""

def real_add_source_update():

    conn = create_connection(database='RealEstateValuation')
    cursor = conn.cursor()
    cursor.execute('''
    
    UPDATE
        q_acq
    SET
        [Type] = real_add.[Construction_Type]
    
    FROM 
        [DEVTEST].[dbo].[Quarterly_Acquisitions_List] q_acq
        LEFT JOIN [RealEstateValuation].[dbo].[REV_REAL_ADDITIONS] real_add
        ON q_acq.MEntity = real_add.MEntity
        WHERE q_acq.MEntity = real_add.MEntity    
    ''')
    cursor.commit()
    conn.close()
    print('Real estate valuations construction type has been updated')

# run update ----
if __name__ == '__main__':
    dlr01_source_update()
    graph_source_update()
    real_add_source_update()
print('All updates ran successfully')






