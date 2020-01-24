import numpy as np
import pandas as pd
import pyodbc
import os
import math

import selenium
from getpass import getuser, getpass

"""
Database connection
"""

# SQL Connection ----
user = "1217543"

# SQL Connection Function ----
def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user and create connection string
    cnxn_str = (
        r"Driver={{SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)

# Smart-sheet connection ----
rea_val_engine = create_connection(database='RealEstateValuation')
closed_acquisitions_query = "SELECT * FROM [RealEstateValuation].[dbo].[Smartsheet_Closed]"
close_acq_df = pd.read_sql_query(closed_acquisitions_query, rea_val_engine)
close_acq_df.sort_values('Close of Escrow', inplace=True)
close_acq_df['Close of Escrow'] = pd.to_datetime(close_acq_df['Close of Escrow'], format='%Y-%m-%d')
rea_val_engine.close()

# Acquisition group creation ----
grp_names = [
    "F15_Q4",
    "F16_Q1",
    "F16_Q2",
    "F16_Q3",
    "F16_Q4",
    "F17_Q1",
    "F17_Q2",
    "F17_Q3",
    "F17_Q4",
    "F18_Q1",
    "F18_Q2",
    "F18_Q3",
    "F18_Q4",
    "F19_Q1",
    "F19_Q2",
    "F19_Q3",
    "F19_Q4",
    "F20_Q1",
    "F20_Q2",
    "F20_Q3",
    "F20_Q4"
]

# Group Numbers ----
grp_num_range = range(1, len(grp_names) + 1)
grp_num = []
for i in grp_num_range:
    grp_num.append(i)

# Quarterly Acquisitions Classification DF ----
grp_dict = dict(zip(grp_num, grp_names))

# Create logic for quarter grp id ----
def grp_number_classification(x):

    """
    :param x: "Close of Escrow" column from smart-sheet api
    :return: new column called "Group" with an object representation of the group number
    """

    # begin classification from F15 Q4
    start_date = pd.to_datetime('2015-01-01')
    end_date = pd.to_datetime('2015-03-31')

    # iterate through every number in the grp_dict ----
    for grp in grp_dict.keys():
        if (x >= start_date) & (x <= end_date):
            return str(grp)
        else:
            # Increment to next quarter and continue the loop ----
            start_date = start_date + pd.offsets.MonthEnd(3)
            end_date = end_date + pd.offsets.MonthEnd(3)
        continue

# Apply the newly created function ----
close_acq_df['Group'] = close_acq_df['Close of Escrow'].apply(grp_number_classification)
close_acq_df.Entity = close_acq_df.Entity.astype(str)
close_acq_df.Entity = close_acq_df['Entity'].apply(lambda x: x.replace(" ", ""))
close_acq_df.Entity = close_acq_df.Entity.astype(object)

#TODO: Update close of escrow date for relevant centers ----
# Update Entity 723077 close of escrow date
close_acq_df[close_acq_df.Entity == '723077'].loc[:, ['Close of Escrow']] = '2019-05-21'

"""
Import previously used entity list 
"""
entity_list = pd.read_excel(r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q2\acquisitions_list.xlsx")

# Update nan profit_center values to 0 and convert to string ----
entity_list.Profit_Center = entity_list.Profit_Center.apply(lambda x: 0 if np.isnan(x) else x)
entity_list.Profit_Center = entity_list.Profit_Center.astype(str)
entity_list.Profit_Center = entity_list.Profit_Center.apply(lambda x: x[:-2] if len(x) > 2 else x)
entity_list.Profit_Center = entity_list.Profit_Center.astype(object)
entity_list.Entity = entity_list.Entity.astype(str)
prev_used_cols = ['MEntity', 'Profit_Center', 'Simple Owner', 'Include?', 'Type', 'Parent MEntity',
                  'Abutting', 'Remote']

# Merge and filter relevant grps ----
entity_list_merge = pd.merge(left=close_acq_df, right=entity_list[prev_used_cols], on='MEntity', how='left')
entity_list_merge.dropna(subset=['Group'], inplace=True)
entity_list_merge.Group = entity_list_merge.Group.astype(int)

#TODO: Incoroporate missing profit center observations ----

# Exclude previously un-used acquisitions F20_Q1 ----
grp_18_below_mask = entity_list_merge.Group <= 18
present_pc_mask = entity_list_merge.Profit_Center.notna()
f20_q1_list = entity_list_merge[grp_18_below_mask & present_pc_mask].copy()

# Re-asses F20_Q2 for additional centers that should have been included ----
grp_19_above_mask = entity_list_merge.Group >= 19
grp_20_below_mask = entity_list_merge.Group <= 20
f20_q2_above_list = entity_list_merge[grp_19_above_mask & grp_20_below_mask]

"""
SAP Hierarchy for Profit Center Numbers
"""
finaccounting_engine = create_connection(database='FinAccounting')
sap_hierarchy_query = "SELECT * FROM SAP_Cost_Center_Hierarchy WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(f20_q2_above_list.MEntity))
sap_hierarchy = pd.read_sql_query(sap_hierarchy_query, finaccounting_engine)
finaccounting_engine.close()
sap_hierarchy.rename(columns={'Profit Center': 'Profit_Center'}, inplace=True)

# Import Profit Center number for observations missing (part 2)----
f20_q2_above_2 = pd.merge(left=f20_q2_above_list, right=sap_hierarchy.loc[:, ['MEntity', 'Profit_Center']], on='MEntity', how='left')
f20_q2_above_2.Profit_Center_x = np.where(f20_q2_above_2.Profit_Center_x.isna(), f20_q2_above_2.Profit_Center_y, f20_q2_above_2.Profit_Center_x)
f20_q2_above_2.drop(['Profit_Center_y'], axis=1, inplace=True)
f20_q2_above_2.rename(columns={'Profit_Center_x': 'Profit_Center'}, inplace=True)

# Drop duplicate MEntity numbers, keep the first appearance (part 3) & Check abutting classification----
f20_q2_above_3 = f20_q2_above_2.drop_duplicates(subset='MEntity', keep='first')
f20_q2_above_3.Abutting_y = f20_q2_above_3.Abutting_y.apply(lambda x: True if x == 'Yes' else False)
f20_q2_above_3.Abutting_x = f20_q2_above_3.Abutting_y.apply(lambda x: True if x == True else False)
f20_q2_above_3.drop(['Abutting_y'], axis=1, inplace=True)
f20_q2_above_3.rename(columns={'Abutting_x': 'Abutting'}, inplace=True)

# Remote classification check: Import Parent MEntity from Graph DB ----
finanalysis_engine = create_connection(database='FINANALYSIS')
graph_query = "SELECT * FROM GRAPH_INDEX_MATCH WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(f20_q2_above_3['MEntity']))
graph_df = pd.read_sql_query(graph_query, finanalysis_engine)
finanalysis_engine.close()
f20_q2_above_3 = pd.merge(left=f20_q2_above_3, right=graph_df.loc[:, ['MEntity', 'Simple Owner','Parent MEntity']],
                      on='MEntity', how='left')
f20_q2_above_3['Parent MEntity_x'] = f20_q2_above_3['Parent MEntity_y']
f20_q2_above_3.drop(['Parent MEntity_y'], axis=1, inplace=True)
f20_q2_above_3.rename(columns={'Parent MEntity_x': 'Parent MEntity'}, inplace=True)

# Simple Owner classification check: Replace current simple owner column with Graph ----
f20_q2_above_3['Simple Owner_x'] = f20_q2_above_3['Simple Owner_y']
f20_q2_above_3.drop(['Simple Owner_y'], axis=1, inplace=True)
f20_q2_above_3.rename(columns={'Simple Owner_x': 'Simple_Owner'}, inplace=True)

# Classify as remote if parent MEntity is present & inclusion in the list ----
f20_q2_above_3['Parent MEntity'] = f20_q2_above_3['Parent MEntity'].apply(lambda x: 'No' if (x is None) | (x is np.nan) else x)
f20_q2_above_3.Remote = f20_q2_above_3['Parent MEntity'].apply(lambda x: 'Yes' if len(x) > 4 else x)
f20_q2_above_3['Include?'] = np.where((f20_q2_above_3['Parent MEntity'] != 'No') | (f20_q2_above_3.Remote == 'Yes'),
                                      'No',
                                      f20_q2_above_3['Include?'])

# Update Columns from existing & concatenate into single DF ----
f20_q1_list.rename(columns={'Abutting_x': 'Abutting',
                            'Simple Owner': 'Simple_Owner'}, inplace=True)
f20_q1_list.drop(['Abutting_y'], axis=1, inplace=True)
entity_list_current = pd.concat([f20_q1_list, f20_q2_above_3], axis=0)

# Missing "simple owner" observations merge (older than F20_Q2) ----
# entity_list_current = pd.merge(left=entity_list_current, right=graph_df.loc[:, ['MEntity', 'Simple Owner']],
#                                on='MEntity', how='left')
# entity_list_current.Simple_Owner = np.where(entity_list_current.Simple_Owner.isna(),
#                                             entity_list_current['Simple Owner'],
#                                             entity_list_current.Simple_Owner)
# entity_list_current.drop(['Simple Owner'], axis=1, inplace=True)

# Check final filters for inclusion in list (only retain True)----
not_duplicate_pc = ~entity_list_current.duplicated('Profit_Center')
owner_uhi = entity_list_current.Simple_Owner == 'UHI'
remote_center = entity_list_current.Remote == 'No'
abutting_center = entity_list_current.Abutting != 'True'
entity_list_current['include'] = not_duplicate_pc & owner_uhi & remote_center & abutting_center
entity_list_current['Include?'] = entity_list_current['include']
entity_list_current.drop(['Include?'], axis=1, inplace=True)

# Export to csv ----
entity_list_current.to_excel(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q3\center_list\acquisitions_list_q3.xlsx',
                             index=False)
