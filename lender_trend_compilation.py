import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime

from income_statement_compilation import income_statement
from sap_db_filter import chart_of_accounts, create_connection

# SQL Upload Packages ----
import sqlalchemy, urllib


"""
Connect to Graph DB  
"""
entity_info_con = create_connection(database="FINANALYSIS")
graph_index_query = "SELECT * FROM [FINANALYSIS].[dbo].[GRAPH_INDEX_MATCH]"
graph_entity_info_query = "SELECT * FROM [FINANALYSIS].[dbo].[GRAPH_ENTITY_INFO]"

# Import Graph Entity List ----
index_match_df = pd.read_sql_query(graph_index_query, entity_info_con)
entity_info_df = pd.read_sql_query(graph_entity_info_query, entity_info_con)
entity_info_con.close()

"""
Life Storage Same Store List
"""
# same_store_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Report Scripts\Storage\scenario_tool\same_store_centers_F20Q3.xlsx')
# same_store_list.rename(columns={'Mentity': 'MEntity'}, inplace=True)
# merged_df = pd.merge(left=same_store_list, right=entity_info_df.loc[:, ['MEntity', 'Cost Center']],
#                      on='MEntity', how='left')
# merged_df.rename(columns={'Cost Center': 'profit_center'}, inplace=True)
#
# # Check for duplicate profit_centers and create list of PC ----
# merged_df['dup_pc'] = merged_df.profit_center.duplicated()
# assert sum(merged_df.dup_pc) == 0, print('duplicate pc detected, do not proceed')
# pc_list = list(merged_df.profit_center)


"""
Chart of Accounts Creation 
"""
sap_accounts = pd.read_csv(
    r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\sap_accounts.csv"
)

sap_accounts.rename(
    columns={"SAP ACC. Number": "Account", "Lender Trend Line Item": "line_item"},
    inplace=True,
)

sap_accounts["Account"] = sap_accounts["Account"].astype("object")

# Retain relevant accounts ----
"""
SAP accounts can be added or removed within this next section. Accounts have been known to be added without notice from the accounting department
"""

included_accounts_mask = sap_accounts["line_item"] != "NOT USED FOR LENDER REPORTING"
sap_accounts = sap_accounts[included_accounts_mask]

# Income Statement Line Item Creation ----
line_items = sap_accounts["line_item"].unique()

# Test DF Container ----
separate_df_container = []

# Create Individual Dataframes of line items ----
for category in line_items:
    account_mask = sap_accounts["line_item"] == category
    separate_df = sap_accounts[account_mask]
    separate_df_container.append(separate_df)

# Create Dictionary of Income Statement Line Items ----
chart_of_accounts = dict(zip(line_items, separate_df_container))

"""
Rob's All Arec Trend Center List 
"""
all_arec_list = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\Lender Trend\all_arec.csv')
all_arec_list.Profit_Center = all_arec_list.Profit_Center.astype(str)
# all_arec_list.Profit_Center = all_arec_list.Profit_Center.str[:-2]
include_mask = all_arec_list.Profit_Center.apply(lambda x: True if len(x) > 5 else False)
all_arec_pc = all_arec_list[include_mask]
pc_list = list(all_arec_pc.Profit_Center)

"""
Lender Trend Pull
"""
sap_engine = create_connection(database='SAP_Data')
lender_trend_query = "SELECT " \
               "sub.[Date], " \
               "sub.[Line Item] as [Line Item], " \
               "SUM(sub.[Amount]) as [value] " \
               "FROM " \
               "(" \
               "SELECT " \
               "[SAP], " \
               "[Account_Number], " \
               "[Line Item], " \
               "[Date], " \
               "[Amount] " \
               "FROM [SAP_Data].[dbo].[Lender_Financing_Trends] " \
               ") sub " \
               "WHERE sub.[Account_Number] in {} " \
               "AND sub.[SAP] in {} " \
               "GROUP BY sub.[Date], sub.[Line Item]".format(tuple(sap_accounts['Walker Account Number']), tuple(pc_list))

sap_db = pd.read_sql_query(lender_trend_query, sap_engine)
sap_db.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\Lender Trend\all_arec_trend_IS.csv',
              index=False)
sap_engine.close()

"""
U-Box Adjustment
"""
sap_engine = create_connection(database='SAP_Data')
ubox_adj_query = "SELECT " \
                 "sub.[Date], " \
                 "sub.[Description], " \
                 "SUM(sub.[Amount]) as [value] " \
                 "FROM " \
                 "(" \
                 "SELECT " \
                 "[Center]," \
                 "[SAP],  " \
                 "[Description], " \
                 "[Account_Number], " \
                 "[Date], " \
                 "[Amount] " \
                 "FROM [SAP_Data].[dbo].[Lender_Financing_Ubox] " \
                 ") sub " \
                 "WHERE sub.[SAP] in {} " \
                 "GROUP BY sub.[Date], sub.[Description]".format(tuple(pc_list))

ubox_query = pd.read_sql_query(ubox_adj_query, sap_engine)
ubox_query.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\Lender Trend\all_arec_ubox_adjustments.csv',
                  index=False)

"""
All Adjustments 
"""
adjustment_query = "SELECT " \
                   "sub.[Date], " \
                   "sub.[Account_Description], " \ 
                   "SUM(sub.[Total_Adjustment]) as [total_adjustment] " \
                   "FROM " \
                   "(" \
                   "SELECT " \
                   "[ID], " \
                   "[SAP_Number], " \
                   "[Date], " \
                   "[Account], " \
                   "[Account_Description], " \
                   "[Total_Adjustment], " \
                   "[Storage_Adjustment], " \
                   "[UMove_Adjustment], " \
                   "[Storage_Split] " \
                   "FROM [SAP_Data].[dbo].[Lender_Financing_Adjustments]" \
                   ") sub " \
                   "WHERE sub.[SAP_Number] in {} " \
                   "GROUP BY sub.[Date] ,sub.[Account_Description]".format(tuple(pc_list))

adjustment_results = pd.read_sql_query(adjustment_query, sap_engine)
adjustment_results.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\Lender Trend\all_arec_total_adj.csv',
                  index=False)

