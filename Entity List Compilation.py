import pandas as pd
import pyodbc
import numpy as np
import datetime

# SQL Database Engines ----
mentity_engine = pyodbc.connect('DRIVER={SQL SERVER}; SERVER=OPSREPORT02.UHAUL.AMERCO.ORG;DATABASE=MEntity;UID=1217543;PWD=Noe543N')
finaccounting_engine = pyodbc.connect('DRIVER={SQL SERVER}; SERVER=OPSREPORT02.UHAUL.AMERCO.ORG;DATABASE=FinAccounting;UID=1217543;PWD=Noe543N')

# STEP 1: Import Previous ----
# Import Existing Quarterly Acquisitions List ----
existing_list = pd.read_csv("Z://group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Master List.csv")
existing_mentity = existing_list["MEntity"]  # Extract MEntity column
existing_mentity.nunique()
# 311 Unique MEntity Numbers

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

else:
  print("All Existing Acquisitions are accounted for, proceed to new quarter group")


#  -------------------------------------------------------------
#  Step 2: Import Newly Closed Acquisitions for the Quarter ----
#  -------------------------------------------------------------
# Import New Acquisitions List ----
closed_acquisitions_f19 = pd.read_excel('//adfs01.uhi.amerco/departments/mia/group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Closed Acquisitions FY 19.xlsx',sheet_name='Closed Acquisitions FY 19')

# Process imported data frame to match "existing_list" format ----
# Determine centers from the new quarter that are not present within the current acquisitions list.
closed_acquisitions_f19 = closed_acquisitions_f19.sort_values(by=['Close of Escrow'])
new_additions = closed_acquisitions_f19[(closed_acquisitions_f19['Close of Escrow'] >= '2018-10-1')]  # Create Max Date

# Check for Unique Values ----
new_additions.Entity.nunique()  # 28 Unique Entity values

# Create Missing Columns ----
new_additions.insert(0, "Group", 16)  # Most Recent Group ----
new_additions.insert(1, "FY", 2019)
new_additions.insert(2, "Quarter", 3)

# Query to import Entity data for new additions ----
dlr01_query_new = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} AND [STATUS] = 'O' ORDER BY [ENTITY_6NO] ASC".format(tuple(new_additions.Entity))
dlr01_new_acquisitions = pd.read_sql_query(dlr01_query_new, mentity_engine)
dlr01_new_acquisitions.rename(columns={'ENTITY_6NO': 'Entity'}, inplace=True)

# Comparison of lists: EDA ----
dlr01_new_acquisitions.Entity.isin(new_additions.Entity.astype(str))

# Duplicate Entity numbers: Removal of Duplicates ----
duplicate_entity = new_additions[new_additions.Entity == 760077]  # Discovered duplicate through .value_counts
new_additions_unique = new_additions.drop_duplicates(subset='Entity', keep='last')  # Removes duplicates from df

# Reformat new_additions_unique data frame to match existing list format ----
cols_existing = list(existing_list.columns.values)
cols_new = list(new_additions.columns.values)
print(cols_existing)  # Existing column structure
print(cols_new)  # New column structure

# Rename new additions columns to match existing ----
dlr01_new_acquisitions.info(verbose=True)

# Column Search Code ----
column_search = dlr01_new_acquisitions.loc[:, dlr01_new_acquisitions.columns.str.contains(".Area")]
column_search.info(verbose=True)

# Left Join required DLR01 columns on New Acquisitions list ----
new_additions_unique.Entity = new_additions_unique.loc[:, "Entity"].astype(str)
new_additions_unique = new_additions_unique.merge(dlr01_new_acquisitions.loc[:, ["Entity", "ENTITY_NAME", "MEntity", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED", "STATUS"]], how='left', on='Entity')

# Re-format new additions column to match existing list ----
new_additions_unique = new_additions_unique[["Group", "FY", "Quarter", "Close of Escrow", "ENTITY_NAME", "MEntity", "Entity", "ENTITY_TYPE", "DATE_OPENED", "DATE_CLOSED", "STATUS", "Address", "City", "State", "Purchase Price", "Property Description"]]
new_additions_unique.MEntity.nunique()


#  ---------------------------------------------------------------
#  Step 3: Import Profit Center, Owner, and Construction_Type ----
#  ---------------------------------------------------------------

sap_hierarchy_query = "SELECT * FROM SAP_Cost_Center_Hierarchy WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_additions_unique.MEntity))
sap_hierarchy = pd.read_sql_query(sap_hierarchy_query, finaccounting_engine)

sap_hierarchy.MEntity.nunique()  # 25 unique MEntity numbers / 28 total new additions

sap_hierarchy.MEntity.value_counts()  # Search for any duplicate values within the MEntity column
# M0000121074 ; 3 observations
# M0000021212 ; 2 observations

# Remove duplicate MEntity numbers ----
sap_hierarchy_unique = sap_hierarchy.drop_duplicates(subset='MEntity', keep='last')

# Left Join Profit Center on New Additions ----
new_additions_unique = new_additions_unique.merge(sap_hierarchy_unique.loc[:, ["MEntity", "Cost Center", "Hierarchy Area"]], how='left', on='MEntity')

# Import REA Val DB for Construction Type ----
rea_val_engine = pyodbc.connect('DRIVER={SQL SERVER}; SERVER=OPSREPORT02.UHAUL.AMERCO.ORG;DATABASE=RealEstateValuation;UID=1217543;PWD=Noe543N')

rea_val_query = "SELECT * FROM REV_REAL_ADDITIONS WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(new_additions_unique["MEntity"]))
rea_val_db = pd.read_sql_query(rea_val_query, rea_val_engine)
rea_val_engine.close()  # Close the SQL Connection

# Extract "Construction_Type" from DB ----
new_additions_unique = new_additions_unique.merge(rea_val_db.loc[:, ["MEntity", "Parent_MEntity", "Property_Type", "Construction_Type"]], how='left', on='MEntity')

# Re-arrange oolumns to match "Existing" list ----
new_additions_unique = new_additions_unique.iloc[:, [0, 1, 2, 3, 4, 5, 6, 16, 7, 8, 9, 10, 11, 12, 13, 14, 17, 20, 19, 15]]
new_additions_unique.rename(columns={'ENTITY_NAME':'Entity_Name', 'Cost Center':'Profit_Center', 'ENTITY_TYPE':'Entity_Type', 'DATE_OPENED':'Date_Opened', 'DATE_CLOSED':'Date_Closed', 'STATUS':'Status', 'Purchase Price':'Purchase', 'Hierarchy Area':'Owner'}, inplace=True)

# Remove Missing Profit Center Centers ----
missing_profit_center_df = new_additions_unique[pd.isnull(new_additions_unique.Profit_Center)]
new_additions_unique = new_additions_unique[-pd.isnull(new_additions_unique.Profit_Center)]

# Explore the "Owner" column ----
new_additions_unique.Owner = new_additions_unique.Owner.str[3:7]
not_UHI = new_additions_unique[new_additions_unique.Owner != 'UHI']  # Property.com does not have an owner listed for the property

# Remove Non-UHI from the list and continue aggregation ----
new_additions_unique = new_additions_unique[-new_additions_unique.MEntity.isin(not_UHI.MEntity)]


#  -------------------------------
#  Finalize Acquisitions List ----
#  -------------------------------

# Filter for Remote / Abutting Properties (use description) ----
remote_properties_test = new_additions_unique[new_additions_unique['Property Description'].str.contains('.mote')]

# Centers needing to be removed (True Remotes)
# M0000140057
# M0000140990
# M0000139691

removed_remotes = new_additions_unique[new_additions_unique.MEntity.isin(['M0000140057', 'M0000140990', 'M0000139691'])]

# Filter Abutting Properties ----
abutting_properties_test = new_additions_unique[new_additions_unique['Property Description'].str.contains('.butting')]

# Centers needing to be removed (True Abutting)
# M0000000400
# M0000003759
# M0000001042
# M0000000906
# M0000021212
# M0000121074

# All of the properties above have been identified as abutting and will be removed from the analysis.
removed_abutting = new_additions_unique[new_additions_unique.MEntity.isin(['M0000000400', 'M0000003759', 'M0000001042', 'M0000000906', 'M0000021212', 'M0000121074'])]

#  ------------------------------
#  Finalize New Quarter List ----
#  ------------------------------
# Remove all remotes / abutting from the df above
new_additions_unique = new_additions_unique[-new_additions_unique.MEntity.isin(removed_remotes.MEntity)]
new_additions_unique = new_additions_unique[-new_additions_unique.MEntity.isin(removed_abutting.MEntity)]

# Drop extra columns from new_additions_unique ----
new_additions_unique = new_additions_unique.drop(columns=["Property_Type", 'Property Description'])
pre_existing_MEntity = new_additions_unique[new_additions_unique.MEntity.isin(existing_list.MEntity)]  # 2 duplicates

# Append New Quarter Additions to Existing List ----
final_list = existing_list.append(new_additions_unique, ignore_index=True)
final_list["Close of Escrow"] = pd.to_datetime(final_list["Close of Escrow"])  # Convert Close of Escrow to Datetime
final_list["Date_Opened"] = pd.to_datetime(final_list["Date_Opened"])

# Drop duplicates (centers already accounted for in previous quarters ----
final_list = final_list.drop_duplicates(subset='MEntity', keep='first')  # Removes any duplicates present in the data set


#  --------------------------------------
#  Maintain Master Acquisitions File ----
#  --------------------------------------
writer = pd.ExcelWriter(
    'Z:/group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Acquisition List Summary.xlsx',
    engine='xlsxwriter')

existing_list.to_excel(writer, sheet_name='Included Acquisitions', index=False)
missing_profit_center_df.to_excel(writer, sheet_name='Missing_PC', index=False)
removed_remotes.to_excel(writer, sheet_name='Removed_Remotes', index=False)
removed_abutting.to_excel(writer, sheet_name='Removed_Abutting', index=False)
not_UHI.to_excel(writer, sheet_name='Not_UHI', index=False)
new_additions_unique.to_excel(writer, sheet_name='Included Acquisitions', startrow=312, index=False, header=False)
pre_existing_MEntity.to_excel(writer, sheet_name='Pre-existing Centers', index=False, header=False)
writer.save()

