import pandas as pd
import numpy as np

from SAP_DB_Filter import create_connection
import re

# Import AREC List (Directly from Smart Sheet) ----
master_arec_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Closed_Acquisitions_AREC.xlsx',
                                 sheet_name='Compiled')

master_arec_list.rename(columns={'Close of Escrow': 'close_of_escrow',
                                 'Permanent Entity #': 'Entity'}, inplace=True)

# Fill missing values with 0 ----
master_arec_list.fillna(0, inplace=True)
master_arec_list['Entity'] = master_arec_list['Entity'].astype(int)
master_arec_list['Entity'] = master_arec_list['Entity'].astype(str)

# Order DF ----
master_arec_list.sort_values('close_of_escrow', inplace=True)

# Filter for Date Floor of F15 Q4 (2015-01-01) ----
f15_q4_floor = master_arec_list['close_of_escrow'] >= '2015-01-01'
f_master = master_arec_list[f15_q4_floor]
f_master.reset_index(inplace=True, drop=True)


"""
Full AREC List (f_master)

Shape: 638
Unique Entity: 571

"""

# Import DLR01 Data ----
dlr01_con = create_connection(database='MEntity')
dlr01_query = "Select * FROM [MEntity].[dbo].[ENTITY_DLR01] WHERE [STATUS] = 'O'"
dlr01_db = pd.read_sql(dlr01_query, dlr01_con)
dlr01_con.close()
dlr01_db.rename(columns={'ENTITY_6NO':'Entity'}, inplace=True)

# Import MEntity from DLR01 ----
f_master_2 = pd.merge(left=f_master, right=dlr01_db, how='left', on='Entity')

# Fill Missing Values ----
f_master_2.fillna(0, inplace=True)

"""
After merging DLR01, there are;

Shape: 638 ; Additional X rows.
Unique Entity: 571
Uniqye MEntity: 469

"""

# Extract Unique MEntity values ----
unique_MEntity = f_master_2['MEntity'].unique()
zero_mask = unique_MEntity != 0
unique_MEntity_2 = unique_MEntity[zero_mask]

# Import Acquisition Type ----
real_con = create_connection(database='RealEstateValuation')
rea_val_query = "SELECT * FROM REV_REAL_ADDITIONS WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(unique_MEntity_2))
rea_add_db = pd.read_sql_query(rea_val_query, real_con)
real_con.close()  # Close the SQL Connection

# Real Additions Column Filters ----
real_add_col = ["MEntity", "Parent_MEntity", "Property_Type", "Construction_Type"]
real_add_db = rea_add_db.loc[:, real_add_col]

# ---------------------------------------------------------------------------------------------------------------------

# Merge Property Type to f_master ----
f_master_3 = pd.merge(left=f_master_2, right=real_add_db, how='left', on='MEntity')
f_master_3.fillna(0, inplace=True)

# Import Graph Data ----
graph_con = create_connection(database='DEVTEST')
graph_query = "SELECT * FROM [DEVTEST].[dbo].[Graph_Updated]"
graph_db = pd.read_sql(graph_query, graph_con)

test_merge = pd.merge(left=f_master_3, right=graph_db, ho)


# Manual Definition of Remote / Abutting ----
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

# Classification based on property description ----
f_master_3['Abutting'], f_master_3['Remote'] = zip(*f_master_3['Property Description'].map(classify_locs))

# Rename Columns ----
f_master_3.rename(columns={'Hierarchy Area': 'Simple Owner',
                               'Cost Center': 'Profit_Center',
                               'Construction_Type': 'Type',
                               'Latitude': 'LOC_LATITUDE',
                               'Longitude': 'LOC_LONGITUDE',
                               'Parent_MEntity': 'Parent MEntity'},
                      inplace=True)

# Filter DataFrame for relevant columns ----
df_col_filter = ['close_of_escrow',
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
                 'Type',
                 'MEntity',
                 'Parent MEntity',
                 'Abutting',
                 'Remote']

f_master_3[df_col_filter]

