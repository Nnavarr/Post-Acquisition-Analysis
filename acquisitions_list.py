import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
from SAP_DB_Filter import create_connection
import re


#%% Initial Data imports ----

user = "1217543"

# Create SQL Engines ----
mentity_engine = create_connection(database="MEntity")
finaccounting_engine = create_connection(database="FinAccounting")
finanalysis_engine = create_connection(database="FINANALYSIS")
rea_val_engine = create_connection(database="RealEstateValuation")

# Import AREC Smart Sheet Acquisitions List ("Compiled" Sheet) ----
arec_smartsheet = pd.read_excel(
    r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\closed_acquisitions_smartsheet_092019.xlsx",
    sheet_name="Compiled",
)

arec_smartsheet.rename(
    columns={
        "Permanent Entity #": "Entity",
        "Purchase Price": "purchase_price",
        "Close of Escrow": "close_of_escrow",
        "Property Name": "property_name",
        "Property Description": "property_description",
        "Q Closed": "quarter",
        "FY-Q": "fy_quarter",
    },
    inplace=True,
)

# Order by Close of Escrow ----
arec_smartsheet.sort_values("close_of_escrow", inplace=True)

# Update 'Entity' to object type ----
arec_smartsheet["Entity"] = arec_smartsheet["Entity"].astype(str)
arec_smartsheet["Entity"] = np.where(
    arec_smartsheet["Entity"].str.len() < 6, False, arec_smartsheet["Entity"]
)

# Identify missing Entity number entries in the AREC Smarsheet ----
missing_entity_smartsheet = arec_smartsheet[arec_smartsheet["Entity"] == False]

# # Export missing entity smartsheet to excel ----
# missing_entity_smartsheet.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\missing_entity_smartsheet_2_09162019.csv')

# Update Entity Column ----
"""
Issue with "Entity" column:
    There are situations where the Entity column contains characters. The goal
    here is to remove any characters from the column to properly extract Entity
    numbers. This will need to be done for accurate joining in subsequent steps
"""

# Min and Max Close of Escrow ----
min_coe = min(arec_smartsheet["close_of_escrow"])
max_coe = max(arec_smartsheet["close_of_escrow"])

# Date Range ----
yrs_dif = max_coe - min_coe
print("approx " + str(yrs_dif / 365))


#%%

# Export duplicate AREC smartsheet entity numbers ----
smartsheet_duplicates = arec_smartsheet["Entity"].value_counts()[
    arec_smartsheet["Entity"].value_counts() > 1
]
smartsheet_duplicates = pd.DataFrame(smartsheet_duplicates)
dup_smartsheet = arec_smartsheet[
    arec_smartsheet["Entity"].isin(smartsheet_duplicates.index)
]

# Export smartsheet duplicates to csv ----
# dup_smartsheet.to_csv(
#     r"Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\duplicate_from_smartsheet_09132019.csv"
# )

# TODO: Create flag for uplicate Entity numbers. Duplicate Entity numbers are a signal that one acquisition is an expansion of current operations ----


#%% Graph Data Compilation ----

"""
Important Considerations
------------------------
Importing the MEntity number will depend on the centers maturity. Because the
process is ultimately revolved around new acquisitions, we will need to source
the MEntity numbers from a "newer" source (i.e., not rely on operations). As
such, the DLR01 process will work nicely.

For the initial SQL upload, we will use Graph file data to import MEntity

Process 1: Import MEntity number from Graph file (using Entity)
Process 2: Import DLR01 using "ENTITY_6NO". The column name will need to be
           updated to match graph

"""

# -------
# DB Info
# -------

"""
Entity Info
-----------
SQL DB: FINANALYSIS
Table: GRAPH_ENTITY_INFO

Python Variable Names
SQL Connection: finanalysis_engine

Issues:
    Graph Entity Info does not contain a "simple owner" column, nor does it
    contain a "Parent MEntity" column


Index Match
-----------
SQL DB: FINANALYSIS
Table: GRAPH_INDEX_MATCH

Python Variable Names
SQL Connection: finanalysis_engine

Importance: We will import "Simple Owner" and "Parent MEntity" as described above

"""

# Entity Info ----
entity_info_query = "SELECT * FROM GRAPH_ENTITY_INFO"
entity_info_db = pd.read_sql_query(entity_info_query, finanalysis_engine)
entity_info_db["Entity"] = entity_info_db["Entity"].astype(str)

# Index Match ---
index_match_query = "SELECT * FROM GRAPH_INDEX_MATCH"
index_match_db = pd.read_sql_query(index_match_query, finanalysis_engine)
index_match_db["Entity"] = index_match_db["Entity"].astype(str)
index_match_db.rename(
    columns={"Simple Owner": "simple_owner", "Parent MEntity": "parent_mentity"},
    inplace=True,
)


# Graph info ----
graph_cols = ["MEntity", "simple_owner", "parent_mentity"]
graph_db = pd.merge(
    left=entity_info_db,
    right=index_match_db.loc[:, graph_cols],
    how="left",
    on="MEntity",
)

# Test DF shape ----
try:
    assert entity_info_db.shape == graph_db.shape
except AssertionError:
    dup_MEntity_bool = graph_db["MEntity"].value_counts() > 1
    dup_MEntity_df = graph_db["MEntity"].value_counts()[dup_MEntity_bool]
    print(dup_MEntity_df)

    # TODO: Create an output that tracks duplicate MEntity numbers within the merge process above.

#%% Merge Graph Data on AREC Smarsheet ----

# Merge with Graph Data ----
graph_cols_arec = ["Entity", "MEntity", "simple_owner", "parent_mentity"]
arec_smartsheet_updated = pd.merge(
    left=arec_smartsheet,
    right=graph_db.loc[:, graph_cols_arec],
    how="left",
    on="Entity",
)

# Check for duplicated when merging graph and AREC Smartsheet ----
try:
    assert arec_smartsheet_updated.shape[0] == arec_smartsheet.shape[0]
except AssertionError:
    original_counts = pd.DataFrame(arec_smartsheet["Entity"].value_counts())
    original_counts.rename(columns={"Entity": "orig_count"}, inplace=True)
    new_counts = pd.DataFrame(arec_smartsheet_updated["Entity"].value_counts())
    new_counts.rename(columns={"Entity": "new_count"}, inplace=True)

    # Concatenate into single DF by index ----
    merge_comp = pd.concat([original_counts, new_counts], axis=1, sort=False)
    merge_comp["delta"] = merge_comp["new_count"] - merge_comp["orig_count"]

    # Duplicate due to merge ----
    duplicate_mask = merge_comp["delta"] >= 1
    duplicate_graph_merge = merge_comp[duplicate_mask]

    # Filter Graph to examine duplicate Entity numbers ----
    duplicate_graph_mask = graph_db["Entity"].isin(duplicate_graph_merge.index)
    duplicate_graph_when_merge_arec = graph_db[duplicate_graph_mask]

    print(duplicate_graph_merge)

"""
Explore Graph & AREC Merge Duplicates
--------------------------------
With duplicates found within the initial merge, it would be worth understanding
where these duplicates are coming from

"""

# # Explore Dupplicte entries when merging graph and AREC smartsheet ----
# duplicate_graph_when_merge_arec.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\duplicate_arec_graph09132019.csv')


"""
Change to MEntity as Primary Key
--------------------------------
With the Graph data / AREC Smartsheet successfully merged, we can now begin step
2 of the aggregation.

a) Use MEntity as the primary key
b) Check for centers already present within the Graph File
    If a center is already present within the Graph process, we can create a flag
    to easily see the

"""


#%% Filter Missing MEntity numbers ----

# Convert Mentity to string ----
arec_smartsheet_updated["MEntity"] = arec_smartsheet_updated["MEntity"].astype(str)

# Check for MEntity presence ----
arec_smartsheet_updated["MEntity_present"] = arec_smartsheet_updated["MEntity"].apply(
    lambda x: False if len(x) < 11 else True
)

# Determine whether 1 id (Entity / MEntity) is missing ----
arec_smartsheet_updated["missing_ids"] = np.where(
    (arec_smartsheet_updated["Entity"] == False)
    | (arec_smartsheet_updated["MEntity_present"] == False),
    True,
    False,
)

# Checkpoint: At this point, we have determined what locations are missing Entity/MEntity ID.
# We can begin analyzing missing values to determine an appropriate course of action.

# Missing id DF ----
missing_id_mask = arec_smartsheet_updated["missing_ids"] == True
missing_ent_mask = arec_smartsheet_updated["Entity"] == False

# missing MEntity but not Entity ----
missing_mentity_arec_updated = arec_smartsheet_updated[
    missing_id_mask & ~missing_ent_mask
]
missing_entity_arec_updated = arec_smartsheet_updated[
    missing_id_mask & missing_ent_mask
]

# missing_mentity_arec_updated.to_csv(
#     r"Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\missing_mentity_09172019.csv"
# )
# missing_entity_arec_updated.to_csv(
#     r"Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\missing_entity_09172019.csv"
# )

"""
Missing Entity Number:
    Properties that are missing Entity numbers appear to be **Vacant Land**

Missing MEntity Number:
    Merge against DLR01 to determine if MEntity number is present there.
        209 Missing MEntity
"""

missing_mentity_arec_updated["Entity"].nunique()


#%% Import DLR01 DB

"""
DLR01 DB Specs
---------------
SQL DB: MEntity
Table: ENTITY_DLR01

Python Variable Names
SQL Connection: mentity_engine

Why?
Graph data is sourced from this DLR01 table. It can provide an earlier sign of a locations presence

However, we can't simply merge on Entity or MEntity due to the re-usage of these numbers.
As additional layer, we will match the location city in order to ensure the accurate MEntity number is pulled

Lastly, due to the squirrely nature of these numbers, we will check whether the MEntity number associated with
the specific location is still active using the 'STATUS' column.

"""

# DLR01 Query ----
dlr01_query = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} ORDER BY [ID] ASC, ENTITY_6NO".format(
    tuple(missing_mentity_arec_updated["Entity"])
)

dlr01_initial = pd.read_sql(dlr01_query, mentity_engine)

# dlr01_initial.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\dlr01_missing_mentity.csv',
# index=False)

# Check for single address in returned DF ----
missing_entity_number = list(missing_mentity_arec_updated["Entity"])
missing_entity_city = list(missing_mentity_arec_updated["City"])
missing_dictionary = dict(zip(missing_entity_number, missing_entity_city))

unique_city_entity_list = []
unique_city_mentity_list = []

for entity, city in missing_dictionary.items():
    temp_mask = dlr01_initial["ENTITY_6NO"] == entity
    mentity = dlr01_initial[temp_mask]["MEntity"]

    # Single MEntity Number Case ----
    if len(mentity) == 1:
        temp_dlr01_query = "SELECT * FROM ENTITY_DLR01 WHERE MEntity = '{}'".format(
            mentity
        )
        temp_dlr01 = pd.read_sql(temp_dlr01_query, mentity_engine)

        # Match location City to determine appropriate MEntity number ----
        # This step is due to re-usage of MEntity numbers ----
        match_loc_city_mask = temp_dlr01["LOC_CITY"] == city
        matched_dlr01 = temp_dlr01[match_loc_city_mask]
        correct_mentity = matched_dlr01["MEntity"]
        unique_city_mentity_list.append(correct_mentity)
        unique_city_entity_list.append(entity)

    # Multiple MEntity Number Case ----
    elif len(mentity) > 1:
        temp_dlr01_query = "SELECT * FROM ENTITY_DLR01 WHERE MEntity in {}".format(
            tuple(mentity)
        )
        temp_dlr01 = pd.read_sql(temp_dlr01_query, mentity_engine)
        match_loc_city_mask = temp_dlr01["LOC_CITY"] == city
        matched_dlr01 = temp_dlr01[match_loc_city_mask]
        correct_mentity = matched_dlr01["MEntity"]
        unique_city_mentity_list.append(correct_mentity)
        unique_city_entity_list.append(entity)

    # No MEntity number case ----
    else:
        next

# Correct MEntity Dictionary ----
missing_mentity_dict = dict(zip(unique_city_entity_list, unique_city_mentity_list))

# Collapse missing MEntity dictionary into pandas DF ----
missing_mentity_df = pd.DataFrame(pd.concat(missing_mentity_dict))
missing_mentity_df.reset_index(inplace=True)
missing_mentity_df.rename(columns={"level_0": "Entity"}, inplace=True)
missing_mentity_df.drop(columns=["level_1"], inplace=True)

unique_mentity = missing_mentity_df.groupby("Entity")["MEntity"].unique()
unique_mentity_df = pd.DataFrame(unique_mentity)

# Export Missing MEntity to Excel ----
# unique_mentity_df.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Acq List\Compilation Log\found_missing_mentity_09172019.csv')

# Append newly found MEntity numbers against ""

# TODO: Merge newly found MEntity numbers with the updated version of the arec smartsheet.
for entity in missing_df:



#%%


#%%
