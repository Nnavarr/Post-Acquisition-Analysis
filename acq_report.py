import pandas as pd
import numpy as np
import pyodbc
import geopy
import os

from sap_db_filter import create_connection, sap_db_query, chart_of_accounts
from income_statement_compilation import income_statement

import sqlalchemy, urllib

# Establish Connection to SQL DB ----
user = '1217543'

"""
Import AREC Smart-sheet 

DB: [DEVTEST]
Table:[arec_smartsheet]
"""

# Create Connection to AREC Smart-sheet ----
devtest_engine = create_connection(database='DEVTEST')
ss_query = 'SELECT * FROM [DEVTEST].[dbo].[arec_smartsheet]'
ss_df = pd.read_sql_query(ss_query, devtest_engine)
devtest_engine.close()

"""
Import Graph File 

DB1: [FINANALYSIS]
Table2: [GRAPH_ENTITY_INFO]

DB2: [FINANALYSIS]
Table2: [GRAPH_ENTITY_INFO]

"""
# Finanalysis Engine ----
finanalysis_engine = create_connection(database='FINANALYSIS')

# Graph Entity Info ----
entity_info_query = "SELECT * FROM GRAPH_ENTITY_INFO"
entity_info_df = pd.read_sql_query(entity_info_query, finanalysis_engine)
entity_info_df["Entity"] = entity_info_df["Entity"].astype(str)

# Index Match Info ----
index_match_query = "SELECT * FROM GRAPH_INDEX_MATCH"
index_match_df = pd.read_sql_query(index_match_query, finanalysis_engine)
index_match_df["Entity"] = index_match_df["Entity"].astype(str)
index_match_df.rename(
    columns={"Simple Owner": "simple_owner", "Parent MEntity": "parent_mentity"},
    inplace=True,
)

# Merge1: Graph info (merge of entity_info & index_match) ----
graph_cols = ["MEntity", "simple_owner", "parent_mentity"]
graph_df = pd.merge(
    left=entity_info_df,
    right=index_match_df.loc[:, graph_cols],
    how="left",
    on="MEntity",
)

# Eliminate unnecessary columns ----
graph_df.drop(['Address', 'City', 'State', 'Entity', 'simple_owner', 'parent_mentity'], axis=1, inplace=True)

# Merge 2: AREC Smart-sheet and Graph DF ----
df_merge = pd.merge(left=ss_df, right=graph_df, on='MEntity', how='left')

#TODO Employ a retroactive check of MEntity number against address present within Graph ----

"""
Import Real Additions Info 

DB: [RealEstateValuation]
Table: [REV_REAL_ADDITIONS]
"""
real_additions_engine = create_connection(database='RealEstateValuation')
real_additions_query = 'SELECT * FROM [RealEstateValuation].[dbo].[REV_REAL_ADDITIONS]'
real_additions_df = pd.read_sql_query(real_additions_query, real_additions_engine)
real_additions_engine.close()

# Merge AREC Smart-sheet and Real Additions for Acquisition Type (Remote & Abutting)
real_add_cols = ['MEntity', 'Property_Type', 'Construction_Type']
df_merge = pd.merge(left=df_merge, right=real_additions_df.loc[:, real_add_cols], on='MEntity', how='left')

"""
Import Missing Cost Center numbers ----

DB: FinAccounting
Table: [SAP_Cost_Center_Hierarchy]
What: Profit Center
"""

# SAP Cost Center Hierarchy DB ----
finaccounting_engine = create_connection(database='FinAccounting')
sap_hierarchy_query = "SELECT * FROM SAP_Cost_Center_Hierarchy WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(df_merge['MEntity']))
sap_hierarchy = pd.read_sql_query(sap_hierarchy_query, finaccounting_engine)

# Remove duplicates ----
sap_hierarchy_unique = sap_hierarchy.drop_duplicates(subset='MEntity', keep='last')
sap_hierarchy_unique.info()

# Filter for Quarterly Acquisitions Scope ----
q_acq_scope = df_merge['close_of_escrow'] >= '2015-01-01'
df_merge = df_merge[q_acq_scope]

# Update cost center name to profit_center ----
df_merge.rename(columns={'Cost Center': 'profit_center'}, inplace=True)

# Filter missing profit_center number and create list of unique MEntity----
missing_pc_filter = df_merge['profit_center'].isna()
miss_pc_df = df_merge[missing_pc_filter]
mentity_array = miss_pc_df['MEntity'].unique()

# SAP Hierarchy unique ----
sap_mask = sap_hierarchy_unique['MEntity'].isin(mentity_array)
sap_numbers_df = sap_hierarchy_unique[sap_mask]

# Merge missing sap numbers into existing DF ----
df_merge = pd.merge(left=df_merge, right=sap_numbers_df.loc[:, ['MEntity', 'Cost Center']], on='MEntity', how='left')
df_merge['profit_center'] = np.where(df_merge['profit_center'].isna(),
                                        df_merge['Cost Center'],
                                        df_merge['profit_center'])

# Eliminate imported Cost Center column ----
df_merge.drop(['Cost Center'], axis=1, inplace=True)

"""
SAP IS Upload
-------------
Now that we have established the necessary profit center numbers, we can upload their income statement into SQL

"""
# List creation ----

# Remove missing PC records ----
df_merge['present'] = df_merge['profit_center'].apply(lambda x: len(str(x)))
pc_filter = df_merge['present'] == 10
df_merge_f = df_merge[pc_filter]

# Create PC array and list ----
pc_array = df_merge_f['profit_center'].unique()
pc_list = list(pc_array)

fiscal_year = [2015, 2016, 2017, 2018, 2019, 2020]
sap_db_container = []

# Filter SAP DB ----
sap_engine = create_connection(database="SAP_Data")

for fy in fiscal_year:
    sap = sap_db_query(profit_center_list=pc_list,
                       fiscal_yr=fy)
    sap_db_container.append(sap)

# Aggregate IS data into single dataframe ----
q_acq_sap = pd.concat([sap_db_container[0],
                       sap_db_container[1],
                       sap_db_container[2],
                       sap_db_container[3],
                       sap_db_container[4],
                       sap_db_container[5]], axis=0)

# Filter for relevant dates ----
date_filter = q_acq_sap['Date'] <= '2019-09-01'
q_acq_sap = q_acq_sap[date_filter]

# Income Statement Compilation ----
is_container = map(lambda x: income_statement(
    profit_center=x,
    sap_data=q_acq_sap,
    line_item_dict=chart_of_accounts), pc_list
)

q_acq_is = [i for i in list(is_container)]
income_statement_dict = dict(zip(pc_list, q_acq_is))
q_acq_is_aggregate = pd.concat(income_statement_dict)

# ----------
# SQL Upload
# ----------

# Traditional SQL Connection protocol - this is still necessary
base_con = (
    "Driver={{ODBC DRIVER 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database=DEVTEST;"
    "UID={};"
    "PWD={};"
).format(user, os.environ.get("sql_pwd"))
con = pyodbc.connect(base_con)

# URLLib finds the important information from our base connection
params = urllib.parse.quote_plus(base_con)

# SQLAlchemy takes all this info to create the engine
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

q_acq_is_aggregate.to_sql('Quarterly_Acquisitions_IS', engine, index=False, if_exists='replace')

#
# """
# Match against previously used list
#
# Current stats: 59 duplicate SAP / MEntity numbers
# Why?: This can be due to subsequent properties; expansions
#
# """
# # Extract profit center list ----
# sum(df_merge['profit_center'].value_counts() > 1)
# sum(df_merge['MEntity'].value_counts() > 1)
#
# # Import previously used list ----
# f20_q1 = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q2\q1_list.csv')
# f20_q1.rename(columns={'Profit_Center': 'profit_center'}, inplace=True)
#
# sum(f20_q1['Profit_Center'].value_counts() > 1) # 5 duplicate MEntity / SAP numbers
#
# # Import previously used classification ----
# q1_cols = ['MEntity', 'profit_center']
#
# test_merge = pd.merge(left=df_merge, right=f20_q1.loc[:, q1_cols], on='MEntity', how='left')
#


