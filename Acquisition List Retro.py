import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

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
    r'UID={};PWD={};').format(getuser(), pwd))

    #return connection object
    return(pyodbc.connect(cnxn_str))

# Create connections to SQL DB ----
dlr01_con = create_connection(database='MEntity')
finaccounting_con = create_connection(database='FinAccounting')
real_add_con = create_connection(database='RealEstateValuation')
finanalysis_con = create_connection(database='FINANALYSIS')

# Import Master Acquisitions List & Append Center Info ----
master_acquisitions_list = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\Acq List\Master_Acquisitions_List.xlsx')

# Query for DLR01 ----
dlr01_query_initial = "SELECT * FROM ENTITY_DLR01 WHERE ENTITY_6NO in {} AND [STATUS] = 'O' ORDER BY [ID] ASC, [MEntity]".format(tuple(master_acquisitions_list.Entity))
  dlr01_db = pd.read_sql(dlr01_query_initial, dlr01_con)
    dlr01_db.rename(columns={'ENTITY_6NO':'Entity'}, inplace=True)  # Rename column to "Entity"
      list(dlr01_db.columns.values) # List of DLR01 columns ----
        dlr01_db.Entity = dlr01_db.Entity.astype('int64')  # Convert to numeric

    # Merged DF ----
    master_acquisitions_list = pd.merge(master_acquisitions_list, dlr01_db.loc[:, ['Entity', 'ENTITY_NAME', 'DISTRICT_NO', 'MCO_NUM', 'LOC_ZIP', 'LOC_LATITUDE', 'LOC_LONGITUDE','MEntity']], how='left', on='Entity')

# Import SAP Hierarchy Data (Profit Center) ----
unique_master_mentity = master_acquisitions_list.MEntity.dropna(how='all') # Create list of MEntity to filter master_mentity on
unique_master_mentity = unique_master_mentity.unique()

sap_hierarchy_query = "SELECT * FROM [SAP_Cost_Center_Hierarchy] WHERE MEntity in {} ORDER BY [MEntity]".format(tuple(unique_master_mentity))
sap_hierarchy = pd.read_sql(sap_hierarchy_query, finaccounting_con)
duplicated_MEntity = sap_hierarchy[sap_hierarchy.duplicated(subset='MEntity')]  # 35 rows are duplicates

# Import Profit Center Numbers from SAP Hierarchy ----
master_acquisitions_list = pd.merge(master_acquisitions_list, sap_hierarchy.loc[:, ['MEntity', 'Profit Center', 'Company Code']], how='left', on='MEntity')  # There are many duplicates within the merge.
master_acquisitions_list = master_acquisitions_list.drop_duplicates(subset='Profit Center', keep='first')

    # Re-arrange columns within list ----
    master_acquisitions_list = master_acquisitions_list.loc[:, ['Group', 'Close of Escrow', 'ENTITY_NAME', 'DISTRICT_NO', 'MCO_NUM', 'Entity', 'Profit Center', 'Address', 'City', 'State', 'LOC_LATITUDE', 'LOC_LONGITUDE', 'CBSA', 'Purchase Price', 'Property Description','Simple Owner', 'Include?', 'Type']]
    closed_acquisitions_f19 = pd.read_excel('//adfs01.uhi.amerco/departments/mia/group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Closed Acquisitions FY 19.xlsx',sheet_name='Closed Acquisitions FY 19')
    closed_acquisitions_f19['Entity'] = closed_acquisitions_f19['Entity'].map(lambda x: str(x)[:6]) #  Remove "Abutting" within Entity column

          closed_acquisitions_f19.Entity = np.where(closed_acquisitions_f19.Entity == 'nan', 0, closed_acquisitions_f19.Entity)  # Replace Nan with 0
          closed_acquisitions_f19['Entity'] = closed_acquisitions_f19['Entity'].astype('int64')  # Convert to Int64

          master_acquisitions_list = pd.merge(master_acquisitions_list, closed_acquisitions_f19.loc[:, ['Entity', 'Property Description']], how='left', on='Entity')
          types_missing = ['TBD', '0', 'nan']

          # Aggregate Property Description under one column ----
          master_acquisitions_list['Property Description_x'] = np.where(master_acquisitions_list['Property Description_x'].isna(), master_acquisitions_list['Property Description_y'], master_acquisitions_list['Property Description_x'])
          master_acquisitions_list['Property Description_x'] = np.where(master_acquisitions_list['Property Description_x'] == 0, master_acquisitions_list['Property Description_y'], master_acquisitions_list['Property Description_x'])
          master_acquisitions_list['Property Description_x'] = np.where(master_acquisitions_list['Property Description_x'] == 'TBD', master_acquisitions_list['Property Description_y'], master_acquisitions_list['Property Description_x'])

          # Remove Excess Prop Description ----
          master_acquisitions_list.rename(columns={'Property Description_x': 'Property Description'}, inplace=True)
          master_acquisitions_list = master_acquisitions_list.drop('Property Description_y', axis=1)

    # Import Simple Owner for nan rows ----
    list(dlr01_db.columns.values)
    master_acquisitions_list['Simple Owner'].isna()  #380 rows

    # Import MEntity from DLR01 ----
    master_acquisitions_list = pd.merge(left=master_acquisitions_list, right=dlr01_db.loc[:, ["Entity", "MEntity"]], on='Entity', how='left')

      # Import Real Additions DB ----
      real_add_query = "SELECT * FROM [REV_REAL_ADDITIONS] WHERE [MEntity] in {}".format(tuple(master_acquisitions_list.MEntity.dropna()))
      real_add_db = pd.read_sql(real_add_query, real_add_con)
        # real_add will have duplicates due to multiple properties using the same MEntity
          # For example, 1 new acq with multiple buildings (separate acquisition costs)

        # master_acquisitions_list = pd.merge(left=master_acquisitions_list, right=real_add_db.loc[:, ["MEntity", "Closing_Date", "Closing_Price", "Parent_MEntity", "Owner_Type"]], on='MEntity', how='left')

        # Graph Info Import ----
        graph_query = "SELECT * FROM [GRAPH_INDEX_MATCH] WHERE [MEntity] in {}".format(tuple(master_acquisitions_list.MEntity.dropna()))
        graph_db = pd.read_sql(graph_query, finanalysis_con)

          master_acquisitions_list = pd.merge(left=master_acquisitions_list, right=graph_db.loc[:, ["MEntity", "Simple Owner", "Parent MEntity"]], on='MEntity', how='left')

          # Replace Simple owner x with simple owner y (y is graph data) ----
          master_acquisitions_list['Simple Owner_x'] = np.where(master_acquisitions_list['Simple Owner_x'].isna(), master_acquisitions_list['Simple Owner_y'], master_acquisitions_list['Simple Owner_x'])
          master_acquisitions_list = master_acquisitions_list.drop('Simple Owner_y', axis=1)
          master_acquisitions_list.rename(columns={'Simple Owner_x': 'Simple Owner'}, inplace=True)

          # Build a column for the center not being included -----
            # Logic, check a few columns for certain strings and extract into a single column
          abutting_pattern = re.compile('\d*'+'abutting')
            test_string = 'The property is Abutting 755058'
            bool(abutting_pattern.search(test_string.lower()))

          # String detection for abutting / remotes ----
          master_acquisitions_list['Abutting'] = master_acquisitions_list['Property Description'].str.find('utting')
            master_acquisitions_list['Abutting'] = master_acquisitions_list['Abutting'].apply(lambda x: 'Yes' if x > 1 else 'No')

          master_acquisitions_list['Remote'] = master_acquisitions_list['Property Description'].str.find('emote')
            master_acquisitions_list['Remote'] = master_acquisitions_list['Remote'].apply(lambda x: 'Yes' if x > 1 else 'No')  # Key work in description ----
              master_acquisitions_list['Remote_2'] = master_acquisitions_list['Parent MEntity'].notnull()  # Check for Parent MEntity ----
              master_acquisitions_list['Remote'] = np.where(master_acquisitions_list['Remote_2'].values == True, 'Yes', master_acquisitions_list['Remote'])  # Replace original remote column
              master_acquisitions_list = master_acquisitions_list.drop('Remote_2', axis=1)

          # Check Construction type "Type" column ----
          master_acquisitions_list = pd.merge(left=master_acquisitions_list, right=real_add_db.loc[:, ['MEntity', 'Construction_Type']], on='MEntity', how='left')  # 384
          master_acquisitions_list['Construction_Type'] = np.where(master_acquisitions_list['Construction_Type'].isna(), master_acquisitions_list['Type'], master_acquisitions_list['Construction_Type'])

          # Final check on "New Build" properties ----
          master_acquisitions_list['New_Build'] = master_acquisitions_list['Property Description'].str.find('bare')
          master_acquisitions_list['Construction_Type'] = np.where(master_acquisitions_list['New_Build'] > 1, "New Build", master_acquisitions_list['Construction_Type'])  # Append "New Build" if condition met
          master_acquisitions_list = master_acquisitions_list.drop('New_Build', axis=1)

          # Filter for Construction Types containing double asterix (**) ----
          asterix_filter = master_acquisitions_list['Construction_Type'].str.find('**')
            asterix_filter =  np.where(asterix_filter.values > 1, True, False)

            asterix_observations = master_acquisitions_list[asterix_filter]

            # Export asterix centers to excel for records ----
            writer = pd.ExcelWriter('Z:/group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Acq List/TBC_Construction.xlsx',engine='xlsxwriter')
            asterix_observations.to_excel(writer, sheet_name='TBC_Construction', index=False)
              writer.save()

          # After a manual check of Construction_Type, all are correct, just remove "**"
          master_acquisitions_list['Construction_Type'] = master_acquisitions_list['Construction_Type'].str.replace("*", "")
            master_acquisitions_list['Type'] = master_acquisitions_list['Construction_Type']
            master_acquisitions_list = master_acquisitions_list.drop('Construction_Type', axis=1)

          # Check for NaN Type Values ----
          missing_type = master_acquisitions_list[master_acquisitions_list['Type'].isna()]
          missing_type_easy = missing_type.loc[:, ['Entity', 'MEntity', 'Property Description']]

          # Manual Check of Construction_Type ----
          updated_types = {'M0000000352': 'Conversion','M0000001314': 'Conversion', 'M0000000175': 'Conversion', 'M0000001362': 'Conversion', 'M0000000933': 'Conversion', 'M0000001119': 'Conversion', 'M0000001218': 'Conversion', 'M0000001218': 'Conversion', 'M0000000448': 'New Build', 'M0000000902':'Conversion', 'M0000001290':'New Build', 'M0000001200':'Conversion', 'M0000139998':'Conversion', 'M0000138978':'New Build', 'M0000138976':'Conversion', 'M0000138975':'Conversion', 'M0000138973':'Conversion', 'M0000139405':'Conversion', 'M0000137076':'New Build', 'M0000000400':'Conversion', 'M0000000906':'New Build'}
          updated_types_df = pd.DataFrame(list((updated_types.items())))
          updated_types_df.rename(columns={0:'MEntity', 1:'Type'}, inplace=True)

          # Merge updated Types into existing master list ----
          master_acquisitions_list['Type'].update(master_acquisitions_list.MEntity.map(updated_types_df.set_index('MEntity').Type))  # Working Code ----

    # Finalize the "Include" Column ----
    include_condition = ~((master_acquisitions_list['Abutting'] == 'Yes') | (master_acquisitions_list['Remote'] == 'Yes') | (master_acquisitions_list['Simple Owner'] == 'SAC')|(master_acquisitions_list['Type'] == 'Abutting' ))
    master_acquisitions_list['Include_Final'] = include_condition

    # Data Frame Comparison (Prior to new changes) -----
    master_acquisitions_list_old = master_acquisitions_list.copy(deep=True)
    master_acquisitions_list['Include?'] = np.where(master_acquisitions_list['Include_Final'] == True, 'Yes', 'No')
    master_acquisitions_list = master_acquisitions_list.drop('Include_Final', axis = 1)

      # Subsequent Profit Center Check ----
      np.sum(master_acquisitions_list['Profit Center'].value_counts() >= 2)  # 19 Profit Center Duplicates ----
      np.sum(master_acquisitions_list['MEntity'].value_counts() >= 2)  # 25 Duplicate MEntity numbers ----

        # There is a mismatch between duplicate profit centers and MEntity numbers ; Will need to check further ----
        # Match Profit Centers to MEntity numbers ----
        d_MEntity_filter = master_acquisitions_list['MEntity'].value_counts()
        d_MEntity_filter = d_MEntity_filter[d_MEntity_filter >= 2]

        d_MEntity_df = pd.DataFrame(d_MEntity_filter)
        d_MEntity_df.rename(columns={'MEntity': 'Count'}, inplace=True)
        d_MEntity_df['MEntity'] = d_MEntity_df.index
        d_MEntity_df = d_MEntity_df.reset_index(drop=True)
        d_MEntity_df = d_MEntity_df.loc[:, ['MEntity', 'Count']]  # 25 Rows

        # Duplicate Profit Center ----
        d_PC_filter = master_acquisitions_list['Profit Center'].value_counts()
        d_PC_filter = d_PC_filter[d_PC_filter >= 2]

        d_PC_df = pd.DataFrame(d_PC_filter)
        d_PC_df.rename(columns={'Profit Center':'Count'}, inplace=True)
        d_PC_df['Profit Center'] = d_PC_df.index
        d_PC_df = d_PC_df.reset_index(drop=True)
        d_PC_df = d_PC_df.loc[:, ["Profit Center", "Count"]]  # 19 rows
          d_PC_df = pd.merge(left=d_PC_df, right=master_acquisitions_list.loc[:, ['Profit Center', 'MEntity']].drop_duplicates(keep='first'), how='left', on='Profit Center')

          # Left Join Profit Center on dupplicate MEntity Numbers ----
          duplicates_df = pd.merge(left= d_MEntity_df, right=d_PC_df, how='left', on='MEntity')


      # Final Comparison of Acquisition lists ----
      final_check = master_acquisitions_list.loc[:, ['MEntity', 'Include?', 'Type', 'Remote', 'Abutting']]
      abutting_final_check = final_check[final_check.Type == 'Abutting']

      # Difference in inclusion columns ----
      previous_included = master_acquisitions_list_old.loc[:, ['MEntity', 'Include?', 'Abutting', 'Remote']]
      new_included = master_acquisitions_list.loc[:, ['MEntity', 'Include?', 'Abutting', 'Remote']]

        # Comparison ----
        new_vs_old = previous_included["Include?"] == new_included["Include?"]
          changed_status_centers = master_acquisitions_list[~new_vs_old]

# Export Findings into Excel ----
#writer2 = pd.ExcelWriter(
  #  'Z:/group/MIA/Noe/Projects/Post Acquisition/Report/Quarterly Acquisitions/Acq List/Acquisition_list.xlsx', engine='xlsxwriter')

#master_acquisitions_list.to_excel(writer2, sheet_name='Master_List', index=False)
#changed_status_centers.to_excel(writer2, sheet_name='Updated_Status', index=False)
#writer2.save()
