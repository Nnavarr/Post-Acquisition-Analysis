import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

user = '1217543'

# SQL Connection Function ----
def create_connection(database):
    #load password from env, entry if not available
    pwd = os.environ.get('sql_pwd')
    if pwd is None:
        pwd = getpass()

    #load user and create connection string
    cnxn_str = ((r'Driver={{SQL Server}};'
    r'Server=OPSReport02.uhaul.amerco.org;'
    r'Database='+database+';'
    r'UID={};PWD={};').format(user, pwd))

    #return connection object
    return(pyodbc.connect(cnxn_str))


mentity_engine = create_connection(database='MEntity')
finaccounting_engine = create_connection(database='FinAccounting')
finanalysis_engine = create_connection(database='FINANALYSIS')
rea_val_engine = create_connection(database='RealEstateValuation')

# ----------------------------------
# Step 1: Import Previous List
# ----------------------------------
existing_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\Acq List\Master_Acquisitions_List.xlsx')

existing_mentity = existing_list["MEntity"]  # Extract MEntity column

# STEP 2: Filter DLR01 With Existing MEntity numbers ----
dlr01_query_initial = "SELECT * FROM ENTITY_DLR01 WHERE MEntity in {} AND [STATUS] = 'O' ORDER BY [ID] ASC, [MEntity]".format(tuple(existing_mentity))
dlr01_initial = pd.read_sql_query(dlr01_query_initial, mentity_engine)

dlr01_unique_mentity = dlr01_initial.MEntity.unique()

# STEP 3: Determine if the center is still open ----
mentity_check = np.average((np.isin(existing_mentity, dlr01_unique_mentity)))
# A11 MEntity numbers are still open, count = 311 (matches line 33)

# Logical check for equivalency ----
if mentity_check < 1:
  print("A center within the Existing Acquisitions list has closed")
  closed_mentity = existing_list[-existing_list.MEntity.isin(dlr01_unique_mentity)]
  print(closed_mentity)

else:
  print("All Existing Acquisitions are accounted for, proceed to new quarter group")


#  -------------------------------------------------------------
#  Step 2: Import New Closed Acquisitions for the Quarter ----
#  -------------------------------------------------------------
# Import New Acquisitions List ----

# ----------------
# Fields to update
# ------------------------------------------------------------------
new_file_path = r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\F19 Q4\New Acquisitions List\Closed Acquisitions FY19.xlsx'
excel_sheet_name = 'Closed Acquisitions FY 19'
# ------------------------------------------------------------------
closed_acquisitions_f19 = pd.read_excel(new_file_path,
                                        sheet_name=excel_sheet_name)

# Process imported data frame to match "existing_list" format ----
closed_acquisitions_f19 = closed_acquisitions_f19.sort_values(by=['Close of Escrow'])
closed_acquisitions_f19.rename(columns={'Permanent Entity #': 'Entity'}, inplace=True)
closed_acquisitions_f19['Entity'] = closed_acquisitions_f19.Entity.astype(str)

new_additions = closed_acquisitions_f19[(closed_acquisitions_f19['Close of Escrow'] >= '2018-12-28')]  # Create Max Date

# Unique Entity values DF ----
num_unique_entity_new_additions = new_additions.Entity.nunique()
unique_entity_new_additions = new_additions.Entity.unique()

# Check for Duplicate Entity values ----
assert max(new_additions.Entity.value_counts()) == 1, 'Duplicate Entity within new additions DF'

# Create Missing Columns ----
new_additions.insert(0, "Group", 17)
new_additions.insert(1, "FY", 2019)
new_additions.insert(2, "Quarter", 4)

# Query to import Entity data for new additions ----
dlr01_query_new = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} AND [STATUS] = 'O' ORDER BY [ENTITY_6NO] ASC".format(tuple(new_additions.Entity))
dlr01_new_acquisitions = pd.read_sql_query(dlr01_query_new, mentity_engine)
dlr01_new_acquisitions.rename(columns={'ENTITY_6NO': 'Entity'}, inplace=True)

# New Addition Entity numbers within dlr01
included_entity = np.isin(unique_entity_new_additions ,dlr01_new_acquisitions.Entity)
assert included_entity.size == unique_entity_new_additions.size, 'A new additions entity is not present within DLR01'

# Duplicate Entity numbers: Removal of Duplicates ----
if sum(new_additions.Entity.duplicated()) >= 1:
    print("Duplicate Entity numbers found within new additions DF")
    # new_additions_unique = new_additions.drop_duplicates(subset='Entity', keep='last')

else:
    print("No duplicate Entity numbers were found within new additions DF")

# Reformat new_additions_unique data frame to match existing list format ----
cols_existing = list(existing_list.columns.values)
cols_new = list(new_additions.columns.values)
print(cols_existing)  # Existing column structure
print(cols_new)  # New column structure

# Left Join required DLR01 columns on New Acquisitions list ----
new_additions_unique = new_additions.merge(dlr01_new_acquisitions.loc[:, ["Entity", "ENTITY_NAME", "MEntity", "MCO_NUM", "DISTRICT_NO", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED", "STATUS"]], how='left', on='Entity')

# Re-format new additions column to match existing list ----
new_additions_unique = new_additions_unique[["Group", "FY", "Quarter", "Close of Escrow", "ENTITY_NAME", "MEntity", "Entity", "MCO_NUM", "DISTRICT_NO", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED", "STATUS", "Address", "City", "State", "Purchase Price", "Property Description"]]
new_additions_unique.MEntity.nunique()

#  ---------------------------------------------------------------
#  Step 3: Import Profit Center, Owner, and Construction_Type ----
#  ---------------------------------------------------------------
sap_hierarchy_query = "SELECT * FROM SAP_Cost_Center_Hierarchy WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_additions_unique.MEntity))
sap_hierarchy = pd.read_sql_query(sap_hierarchy_query, finaccounting_engine)

sap_hierarchy.MEntity.nunique()
sap_hierarchy.MEntity.value_counts()  # Search for any duplicate values within the MEntity column
# 4-22-2019 Duplicate MEntity Numbers
# M0000001195 ; 2 observations
# M0000139516 ; 2 observations

sap_hierarchy_unique = sap_hierarchy.drop_duplicates(subset='MEntity', keep='last')

# Left Join Profit Center on New Additions ----
new_additions2 = new_additions_unique.merge(sap_hierarchy_unique.loc[:, ["MEntity", "Cost Center", "Hierarchy Area"]], how='left', on='MEntity')

# -----------------------------------
# Step 4: Real Additions Import  ----
# -----------------------------------
rea_val_query = "SELECT * FROM REV_REAL_ADDITIONS WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_additions_unique["MEntity"]))
rea_val_db = pd.read_sql_query(rea_val_query, rea_val_engine)
rea_val_engine.close()  # Close the SQL Connection

# Extract "Construction_Type" from DB ----
new_additions3 = new_additions2.merge(rea_val_db.loc[:, ["MEntity", "Parent_MEntity", "Property_Type", "Construction_Type"]], how='left', on='MEntity')

# Create DF of missing profit centers (to be exported separately) ----
missing_profit_center_df = new_additions3[pd.isnull(new_additions3['Cost Center'])]

# Explore the Hierarchy / Owner column ----
new_additions3['Hierarchy Area'] = new_additions3['Hierarchy Area'].str[3:7]

# Create separate DF for non UHI owners ----
not_UHI = new_additions3[new_additions3['Hierarchy Area'] != 'UHI']

# --------------------
# Step 5: Graph Import
# --------------------
graph_query = "SELECT * FROM GRAPH_ENTITY_INFO WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(existing_mentity))
graph_loc = pd.read_sql_query(graph_query, finanalysis_engine)

#  Left merge long/lat ----
new_additions3 = pd.merge(new_additions3,graph_loc.loc[:, ["MEntity", "Latitude", "Longitude", 'CBSA']], how='left', on='MEntity')

# --------------------------------------
# Step 6: Remote/Abutting Classification
# --------------------------------------
# Classify Remote/Abutting Properties ----
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

# Further classifications and data filtering ----
new_additions3['Abutting'], new_additions3['Remote'] = zip(*new_additions3['Property Description'].map(classify_locs))

# ---------------------
# Step 7: Finalize List
# ---------------------
# Rename New Acquisitions Columns ----
new_additions3.rename(columns={'Hierarchy Area': 'Simple Owner',
                               'Cost Center': 'Profit_Center',
                               'Construction_Type': 'Type',
                               'Latitude': 'LOC_LATITUDE',
                               'Longitude': 'LOC_LONGITUDE',
                               'Parent_MEntity': 'Parent MEntity'},
                      inplace=True)

new_additions3['Include?'] = 0
new_additions3['Include?'] = np.where((new_additions3['Abutting'] == 'No') & (new_additions3['Remote'] == 'No'), 'Yes', 'No')
new_additions3['Include?'] = np.where(new_additions3['Simple Owner'] != 'UHI', 'No', new_additions3['Include?'])

# Match Existing List Column Order ----
new_additions4 = new_additions3.loc[:, ['Group',
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
final_list = existing_list.append(new_additions4, ignore_index=True, sort=False)

# ------------------------------------
# Final Step: Export List to Directory
# ------------------------------------
# final_list.to_excel(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\F19 Q4\New Acquisitions List\Master_Acquisitions_List.xlsx',
#                     index=False)
