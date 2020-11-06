mport pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

from IS_function import income_statement
from sap_db_filter import sap_db_query, chart_of_accounts

# Packages necessary for pd.to_sql() functionality
import sqlalchemy, urllib

# establish connections to SQL ----
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

def graph_center_list():

    # import all AREC list; defined by simple owner = UHI ----
    graph_engine = create_connection(database='Graph')
    index_match_query = "SELECT " \
                        "index_match.[Entity], " \
                        "index_match.[MEntity], " \
                        "index_match.[Parent MEntity], " \
                        "entity_info.[Cost Center], " \
                        "index_match.[Simple Owner], " \
                        "entity_info.[Name], " \
                        "entity_info.[Address], " \
                        "entity_info.[City], " \
                        "entity_info.[State], " \
                        "entity_info.[CBSA], " \
                        "index_match.[District], " \
                        "index_match.[MCO] " \
                        "FROM [Graph].[dbo].[Index Match] index_match " \
                        "LEFT JOIN [Graph].[dbo].[Entity Info] entity_info " \
                        "ON index_match.MEntity = entity_info.MEntity " \
                        "WHERE index_match.[Simple Owner] = 'UHI'"

    index_match_df = pd.read_sql_query(index_match_query, graph_engine)
    graph_engine.close()
    mentity_num, profit_center_num = list(index_match_df.MEntity.unique()), list(index_match_df['Cost Center'].unique())

    return mentity_num, profit_center_num

def income_statement_aggregation(pc_list):

    # fiscal year list
    fy_list = [2020]
    is_container = []

    for fy in fy_list:
        sap_data = sap_db_query(pc_list, fiscal_yr=fy)
        is_container.append(sap_data)

    sap_db = pd.concat(is_container)

    # income statement compilation
    is_container = map(
        lambda x: income_statement(
            profit_center=x, sap_data=sap_db, line_item_dict=chart_of_accounts
        ),
        pc_list
    )

    # list compilation and extraction
    is_list = [n for n in list(is_container)]
    is_dict = dict(zip(pc_list, is_list))
    agg_df = pd.concat(is_dict)

    return agg_df

def all_arec_upload(arec_df):

    base_con = (
        "Driver={{ODBC DRIVER 17 for SQL Server}};"
        "Server=OPSReport02.uhaul.amerco.org;"
        "Database=DEVTEST;"
        "UID={};"
        "PWD={};"
    ).format(user, os.environ.get("sql_pwd"))
    # con = pyodbc.connect(base_con)
    params = urllib.parse.quote_plus(base_con)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    arec_df.to_sql('Center_IS', engine, if_exists='append', index=False)
    engine.close()

    return print("The all arec upload has been uploaded successfully.")

# test functions are working as intended ----
mentity_list, pc_list = graph_center_list()
is_agg = income_statement_aggregation(pc_list)
all_arec_upload(is_agg)

if __name__ == '__main__':
    try:
        # income statement aggregation & upload
        mentity_list, pc_list = graph_center_list()
        is_agg = income_statement_aggregation(pc_list)
        all_arec_upload(is_agg)

    except:
        print("There was an issue with the all AREC upload. Please review and try again.")
