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
arec_smartsheet["Entity"] = arec_smartsheet["Entity"].astype(str).str[:-2]
arec_smartsheet["Entity"] = np.where(
    arec_smartsheet["Entity"].str.len() < 6, np.nan, arec_smartsheet["Entity"]
)

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
Process 2: Import MEntity number from DLR01 using "ENTITY_6NO"
                The column name will need to be updated to match graph

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
    print(duplicate_graph_merge)

# Filter Graph to examine duplicate Entity numbers ----
duplicate_graph_mask = graph_db["Entity"].isin(duplicate_graph_merge.index)
duplicate_graph_when_merge_arec = graph_db[duplicate_graph_mask]

# TODO: Export Duplicate Entity when merging Graph and Smartsheet into Excel

"""
Change to MEntity as Primary Key
--------------------------------
With the Graph data / AREC Smartsheet successfully merged, we can now begin step
2 of the aggregation.

a) Use MEntity as the primary key
b) Check for centers already present within the Graph File
    If a center is already present within the Graph process, we can simply
    revert to this for tracking purposes.

"""

#%% Filter Missing MEntity numbers ----


#%%

"""
DLR01 DB Specs
---------------
SQL DB: MEntity
Table: ENTITY_DLR01

Python Variable Names
SQL Connection: mentity_engine
"""

# Import DLR01 Data ----
# Filters on existing MEntity numbers --> will require MEntity numbers to begin wtih

dlr01_query = "SELECT * FROM ENTITY_DLR01 WHERE MEntity in {} AND [STATUS] = 'O' ORDER BY [ID] ASC, [MEntity]".format(
    tuple(existing_mentity)
)


#%%
