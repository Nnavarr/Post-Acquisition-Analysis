import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime as dt

# Packages necessary for pd.to_sql() functionality
import sqlalchemy, urllib

from SAP_DB_Filter import create_connection

# -------------------------------------------
# Create SQL Connection & Download Table Data
# -------------------------------------------
user = '1217543'
devtest_con = create_connection(database='DEVTEST')

list_query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]"
master_list = pd.read_sql(list_query, devtest_con)
devtest_con.close()

# base_con = (
#     'Driver={{ODBC DRIVER 17 for SQL Server}};'
#     'Server=OPSReport02.uhaul.amerco.org;'
#     'Database=DEVTEST;'
#     'UID={};'
#     'PWD={};'
# ).format(user, os.environ.get("sql_pwd"))
# con = pyodbc.connect(base_con)
#
# #URLLib finds the important information from our base connection
# params = urllib.parse.quote_plus(base_con)
#
# #SQLAlchemy takes all this info to create the engine
# engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

# master_list.to_sql('Quarterly_Acquisitions_List', engine, index=False, if_exists='append')

# Import New Acquisitions List ----
min_date = pd.to_datetime('2019-04-01', format='%Y-%m-%d')
max_date = pd.to_datetime('2019-07-01', format='%Y-%m-%d')

new_list = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Closed Acquisitions FY 20.csv')

# Process imported data frame to match "existing_list" format ----
new_list['Close of Escrow'] = pd.to_datetime(new_list['Close of Escrow'], format='%m/%d/%y')
new_list = new_list.sort_values(by=['Close of Escrow'])
new_list.rename(columns={'Permanent Entity #': 'Entity'}, inplace=True)
new_list['Entity'] = new_list.Entity.astype(str)

# Apply date filter (if necessary) ----
min_date_mask = new_list['Close of Escrow'] >= min_date
max_date_mask = new_list['Close of Escrow'] < max_date

new_list = new_list[min_date_mask & max_date_mask]

# Check for Duplicate Entity values ----
assert max(new_list.Entity.value_counts()) == 1, 'Duplicate Entity within new_list DF'

# Create group columns ----
grp_num = 18
grp_fy = 2020
grp_quarter = 1

new_list.insert(0, "Group", grp_num)
new_list.insert(1, "FY", grp_fy)
new_list.insert(2, "Quarter", grp_quarter)

# Unique Entity numbers ----
unique_entity = new_list['Entity'].unique()

# Checkpoint: List ready for additional Entity info import ----

# -------------
# DLR01 Import
# -------------
mentity_engine = create_connection(database='MEntity')

dlr01_query_new = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} AND [STATUS] = 'O' ORDER BY [ENTITY_6NO] ASC".format(tuple(new_list.Entity))
dlr01_new_acquisitions = pd.read_sql_query(dlr01_query_new, mentity_engine)
dlr01_new_acquisitions.rename(columns={'ENTITY_6NO': 'Entity'}, inplace=True)

# New addition entity numbers ----
included_entity = np.isin(unique_entity, dlr01_new_acquisitions['Entity'])
assert included_entity.size == unique_entity.size, 'A new additions entity is not present within DLR01'

mentity_engine.close()

# -----------------
# Reformat new_list
# -----------------
cols_existing = list(master_list.columns.values)
cols_new = list(new_list.columns.values)

# Relevant DLR01 Column names ----
dlr01_cols = ["Entity", "ENTITY_NAME", "MEntity", "MCO_NUM", "DISTRICT_NO", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED","STATUS"]

# Left Join required DLR01 columns on new_list ----
new_list = new_list.merge(dlr01_new_acquisitions.loc[:, dlr01_cols], how='left', on='Entity')

# Re-format new additions column to match existing list ----
new_list = new_list[["Group", "FY", "Quarter", "Close of Escrow", "ENTITY_NAME", "MEntity", "Entity", "MCO_NUM", "DISTRICT_NO", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED", "STATUS", "Address", "City", "State", "Purchase Price", "Property Description"]]


#  --------------------------------------------------
#  Import Profit Center, Owner, and Construction_Type
#  --------------------------------------------------
finaccounting_engine = create_connection(database='FinAccounting')

sap_hierarchy_query = "SELECT * FROM SAP_Cost_Center_Hierarchy WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_list.MEntity))
sap_hierarchy = pd.read_sql_query(sap_hierarchy_query, finaccounting_engine)

sap_hierarchy_unique = sap_hierarchy.drop_duplicates(subset='MEntity', keep='last')

# Left Join Profit Center on New Additions ----
new_list = new_list.merge(sap_hierarchy_unique.loc[:, ["MEntity", "Cost Center", "Hierarchy Area"]], how='left', on='MEntity')

# ---------------------
# Real Additions Import
# ---------------------

rea_val_engine = create_connection(database='RealEstateValuation')
rea_val_query = "SELECT * FROM REV_REAL_ADDITIONS WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_list["MEntity"]))
rea_val_db = pd.read_sql_query(rea_val_query, rea_val_engine)
rea_val_engine.close()  # Close the SQL Connection

# Duplicate MEntity in rea_val ----
# TODO: Create a dynamic way to capture duplicate MEntity numbers from this step and have it be an output
# Duplicate MEntuty = 'M0000121073'

# Drop Duplicate
rea_val_db.drop_duplicates(subset='MEntity', keep='last', inplace=True)

# Extract "Construction_Type" from DB ----
new_list = new_list.merge(rea_val_db.loc[:, ["MEntity", "Parent_MEntity", "Property_Type", "Construction_Type"]], how='left', on='MEntity')

# Create DF of missing profit centers (to be exported separately) ----

# Explore the Hierarchy / Owner column ----
new_list['Hierarchy Area'] = new_list['Hierarchy Area'].str[3:7]


# ------------
# Graph Import
# ------------

finanalysis_engine = create_connection(database='FINANALYSIS')
graph_query = "SELECT * FROM GRAPH_ENTITY_INFO WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_list['MEntity']))
graph_db = pd.read_sql_query(graph_query, finanalysis_engine)

#  Left merge long/lat ----
new_list = pd.merge(new_list, graph_db.loc[:, ["MEntity", "Latitude", "Longitude", 'CBSA']], how='left', on='MEntity')

# ---------------------------
# Remote / Abutting Properties
# ----------------------------

def classify_locs(description):
    description = description.lower()
    # Set default values
    abutting = 'No'
    remote = 'No'
    # Compile regex statements for classifiers
    re_remote = {}
    re_abutting = re.compile(r'abutting\s{0,2}\d+')
    re_remote[0] = re.compile(r'remote([^ly])')
    re_remote[1] = re.compile(r'run\s{0,2}from\s{0,2}\d+')
    re_remote[2] = re.compile(r'\d+.*run.*remote(ly){0,1}')
    re_remote[3] = re.compile(r'managed\s{0,2}remote(ly){0,1}')
    # Change flag to "Yes" if string is found
    if re_abutting.findall(description):
        abutting = 'Yes'
    for item in re_remote.values():
        if item.findall(description):
            remote = 'Yes'
            break
    return(abutting, remote)

# Create Abutting column ----
new_list['Abutting'], new_list['Remote'] = zip(*new_list['Property Description'].map(classify_locs))


# -------------
# Finalize List
# -------------

# Rename New Acquisitions Columns ----
new_list.rename(columns={'Hierarchy Area': 'Simple Owner',
                               'Cost Center': 'Profit_Center',
                               'Construction_Type': 'Type',
                               'Latitude': 'LOC_LATITUDE',
                               'Longitude': 'LOC_LONGITUDE',
                               'Parent_MEntity': 'Parent MEntity'},
                      inplace=True)

new_list['Include?'] = 0
new_list['Include?'] = np.where((new_list['Abutting'] == 'No') & (new_list['Remote'] == 'No'), 'Yes', 'No')
new_list['Include?'] = np.where(new_list['Simple Owner'] != 'UHI', 'No', new_list['Include?'])

new_list = new_list.loc[:, ['Group',
                                 'Close of Escrow',
                                 'ENTITY_NAME',
                                 'DISTRICT_NO',
                                 'MCO_NUM',
                                 'Entity',
                                 'Profit_Center',
                                 'Address',
                                 'City',
                                 'State',
                                 'LOC_LATITUDE',
                                 'LOC_LONGITUDE',
                                 'CBSA',
                                 'Purchase Price',
                                 'Property Description',
                                 'Simple Owner',
                                 'Include?',
                                 'Type',
                                 'MEntity',
                                 'Parent MEntity',
                                 'Abutting',
                                 'Remote']]

# Append New Acquisitions
final_list = master_list.append(new_list, ignore_index=True, sort=False)
final_list.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\f20_master_list.csv',
                  index=False)
