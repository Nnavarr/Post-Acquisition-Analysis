import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime
import xlwings as xl
from textwrap import dedent

from IS_function import income_statement
from sap_db_filter import chart_of_accounts, create_connection

# SQL Upload Packages ----
import sqlalchemy, urllib

"""
Chart of Accounts Creation
"""
sap_accounts = pd.read_csv(
    r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\2020\Post Acquisition\Quarterly Acquisitions\Script_Inputs\sap_accounts.csv"
)
sap_accounts.rename(
    columns={"SAP ACC. Number": "Account", "Lender Trend Line Item": "line_item"},
    inplace=True,
)
sap_accounts["Account"] = sap_accounts["Account"].astype("object")

"""
Set XLWings WB
"""
wb = xl.Book(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\2020\Post Acquisition\Quarterly Acquisitions\Spreadsheet Templates\LS_samestore_IS.xlsx')
trend_sheet = wb.sheets('trend_is')
trend_adj = wb.sheets('trend_adj')
trend_ubox = wb.sheets('trend_ubox_adj')

"""
Connect to Graph DB
"""
entity_info_con = create_connection(database="Graph")
graph_index_query = "SELECT * FROM [Graph].[dbo].[Index Match]"
graph_entity_info_query = "SELECT * FROM [Graph].[dbo].[Entity Info]"

# Import Graph Entity List ----
index_match_df = pd.read_sql_query(graph_index_query, entity_info_con)
entity_info_df = pd.read_sql_query(graph_entity_info_query, entity_info_con)
entity_info_con.close()

"""
Life Storage Same Store List
"""
same_store_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\2020\Post Acquisition\Quarterly Acquisitions\Script_Inputs\life_storage_samestore.xlsx')

# check for duplicate profit center numbers
assert same_store_list.profit_center.nunique() == same_store_list.shape[0]
pc_list = list(same_store_list.profit_center)

"""
Trend Data
"""
sap_engine = create_connection(database='SAP_Data')
lender_trend_query = dedent("""

    SELECT
       sub.[Date],
       sub.[Line_Item] as [Line_Item],
       SUM(sub.[Amount]) as [value]
    FROM
       (
       SELECT
           [SAP_Number],
           [Account_Number],
           [Line_Item],
           [Date],
           [Amount]
       FROM [SAP_Data].[dbo].[Lender_Financing_Trends]
       ) sub 
    WHERE sub.[Account_Number] in {}
    AND sub.[SAP_Number] in {}
    GROUP BY sub.[Date],
    sub.[Line_Item]""".format(tuple(sap_accounts['Walker Account Number']), tuple(pc_list))

sap_db = pd.read_sql_query(lender_trend_query, sap_engine)
# query and export to excel ----
trend_sheet.range('A1:E1').value = "" #clear existing data
trend_sheet.range('A1').options(index=False).value = sap_db

"""
Total Adjustments
"""
adjustment_query = "SELECT " \
                   "sub.[Date], " \
                   "sub.[Account_Description], " \
                   "SUM(sub.[Total_Adjustment]) as [total_adjustment] " \
                   "FROM " \
                   "(" \
                   "SELECT " \
                   "[SAP_Number], " \
                   "[Date], " \
                   "[Account_Number], " \
                   "[Account_Description], " \
                   "[Total_Adjustment], " \
                   "[Storage_Adjustment], " \
                   "[UMove_Adjustment], " \
                   "[Storage_Split] " \
                   "FROM [SAP_Data].[dbo].[Lender_Financing_Adjustments]" \
                   ") sub " \
                   "WHERE sub.[SAP_Number] in {} " \
                   "GROUP BY sub.[Date] ,sub.[Account_Description]".format(tuple(pc_list))

# query and export to excel ----
adjustment_results = pd.read_sql_query(adjustment_query, sap_engine)
trend_adj.range('A1:E1').value = ""
trend_adj.range('A1').options(index=False).value = adjustment_results

"""
U-Box Adjustments
"""
ubox_adj_query = "SELECT " \
                 "sub.[Date], " \
                 "sub.[Account_Description], " \
                 "SUM(sub.[Amount]) as [value] " \
                 "FROM " \
                 "(" \
                 "SELECT " \
                 "[SAP_Number],  " \
                 "[Account_Description], " \
                 "[Account_Number], " \
                 "[Date], " \
                 "[Amount] " \
                 "FROM [SAP_Data].[dbo].[Lender_Financing_Ubox] " \
                 ") sub " \
                 "WHERE sub.[SAP_Number] in {} " \
                 "GROUP BY sub.[Date], sub.[Account_Description]".format(tuple(pc_list))

ubox_query = pd.read_sql_query(ubox_adj_query, sap_engine)
trend_ubox.range('A1:E1').value = ""
trend_ubox.range('A1').options(index=False).value = ubox_query

# close sql connection ----
sap_engine.close()
