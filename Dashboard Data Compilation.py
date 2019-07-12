import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re


# ----------------
# Data Connections
# ----------------
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

# SAP Data Connection ----
sap_engine = create_connection(database='SAP_Data')

# SAP Account Numbers ----
sap_accounts = pd.read_csv(
    r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\sap_accounts.csv')

# Entity List ----
entity_list = pd.read_excel(
    r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\Master_Acquisitions_List.xlsx')


# -----------------------------
# Data Processing: SAP_Accounts
# -----------------------------
sap_accounts.rename(columns={'SAP ACC. Number': 'Account',
                             'Lender Trend Line Item': 'line_item'}, inplace=True)
sap_accounts['Account'] = sap_accounts['Account'].astype('object')

# Retain relevant accounts ----
"""
SAP accounts can be added or removed within this next section. Accounts have been known to be added without notice from the accounting department
"""

included_accounts_mask = sap_accounts['line_item'] != 'NOT USED FOR LENDER REPORTING'
sap_accounts = sap_accounts[included_accounts_mask]


# ----------------------------
# Data Processing: Entity List
# ----------------------------
entity_list['Profit_Center'] = entity_list['Profit_Center'].fillna(0).astype('int64')
entity_list['Profit_Center'] = entity_list['Profit_Center'].astype('object')
entity_in = entity_list[entity_list['Include?'] == 'Yes']

# Duplicate Profit Center Check ----
assert sum(entity_in.duplicated(subset='Profit_Center')) == 0, 'Duplicate Profit Centers Present, DO NO PROCEED'

# -----------------------
# Data Processing: SAP DB
# -----------------------
sap_db_query = 'SELECT * FROM [SAP_Data].[dbo].[FAGLFLEXT] WHERE [PROFIT_CENTER] in {} AND [GL_ACCOUNT] in {}'.format(tuple(entity_in['Profit_Center']), tuple(sap_accounts['Account']))
sap_db = pd.read_sql_query(sap_db_query, sap_engine)
sap_engine.close()

sap_db = sap_db.select(lambda x: not re.search('LC\w', x), axis=1)
sap_db.rename(columns={'GC_PER_1': '04',
                       'GC_PER_2': '05',
                       'GC_PER_3': '06',
                       'GC_PER_4': '07',
                       'GC_PER_5': '08',
                       'GC_PER_6': '09',
                       'GC_PER_7': '10',
                       'GC_PER_8': '11',
                       'GC_PER_9': '12',
                       'GC_PER_10': '01',
                       'GC_PER_11': '02',
                       'GC_PER_12': '03',
                       'PROFIT_CENTER': 'Profit_Center',
                       'GL_ACCOUNT': 'Account',
                       'FISCAL_YEAR': 'Fiscal_Year'}, inplace=True)

# Date column creation ----
sap_db = sap_db.melt(id_vars=['Fiscal_Year', 'Profit_Center', 'Account','CO_AREA', 'TRANS_CURR', 'COMPANY_CODE', 'SEGMENT', 'COST_CENTER'])
sap_db.rename(columns={'variable': 'month'}, inplace=True)

# Create a function to generate Date Column based on
sap_db['month'] = sap_db['month'].astype('int64')
sap_db['Fiscal_Year'] = sap_db['Fiscal_Year'].astype('int64')
sap_db['year'] = np.where((sap_db['month'] >= 4) & (sap_db['month'] <= 12), sap_db['Fiscal_Year'] - 1, sap_db['Fiscal_Year'])
sap_db['month'] = sap_db['month'].astype('object')
sap_db['year'] = sap_db['year'].astype('object')
sap_db['Date'] = sap_db['year'].map(str) + '-' + sap_db['month'].map(str) + '-01'
sap_db['Date'] = pd.to_datetime(sap_db['Date'], format='%Y-%m-%d')

# ---------------------------------------------------------
# Checkpoint: SAP DF is ready for income statement function
# ---------------------------------------------------------

# -----------------
# Chart of Accounts
# -----------------

# Income Statement Line Item Creation ----
line_items = sap_accounts['line_item'].unique()

# Test DF Container ----
separate_df_container = []

# Create Individual Dataframes of line items ----
for category in line_items:
    account_mask = sap_accounts['line_item'] == category
    separate_df = sap_accounts[account_mask]
    separate_df_container.append(separate_df)

# Create Dictionary of Income Statement Line Items ----
chart_of_accounts = dict(zip(line_items, separate_df_container))


# Profit Center Dictionary ----
#TODO: incorporate the imported entity master list Profit Center List ----
pc_numbers = ['7000010541', '7000010764']



# ----------------------------
# Income Statement Compilation
# ----------------------------
"""
Relevant Data Sources: sap_db
Chart of Accounts Dict: chart_of_accounts

"""

# # Test Two Profit Centers ----
# pc_test = '7000010541', '7000010764'

pc_df_container = []

# Step 1: Create Individual Income Statement ----
# TODO: Update this code to be an input once wrapped in a function; replace Profit Center
pc_mask = sap_db['Profit_Center'] == '7000010541'
temp_sap = sap_db[pc_mask]

# Chart of accounts into list ----
coa_acct_list = [n.Account.astype(str) for n in chart_of_accounts.values()]

# Individual Line Item DF Compilation ----
line_item_db = []
for i, name in zip(coa_acct_list, line_items):

    # Filter SAP Data for individual line item DF ----
    temp_data = temp_sap[temp_sap['Account'].isin(i)]

    # Aggregate by Date and Account ----
    temp_data = temp_data.groupby(['Date'])['value'].agg('sum')
    temp_data = pd.DataFrame(temp_data)
    temp_data.reset_index(inplace=True)

    # Create a line item name column ----
    temp_data['lineitem'] = name

    # Append to Container ----
    line_item_db.append(temp_data)

    continue

# Flatten list of individual line items ----
line_item_df = pd.concat(line_item_db)
line_item_df.reset_index(inplace=True, drop=True)

# SAP Sign Flip due to presentation format ----
line_item_df['value'] = np.where(line_item_df['lineitem'].isin(line_items[0:9]),
                                 line_item_df['value'] * -1,
                                 line_item_df['value'])

# Calculate Net Operating Income ----
test_melt = line_item_df

test_pivot = test_melt.pivot(index='Date', columns='lineitem', values='value').fillna(0)

# Missing Line Items ----
missing_mask = np.isin(line_items, test_pivot.columns.values)
missing_line = line_items[~missing_mask]

# Create Place Holder Values for missing line items ----
for i in missing_line:
    test_pivot[i] = 0.00
    continue

# Calculate U-Box Income ----
test_pivot['U-BOX INCOME'] = test_pivot['U-BOX STORAGE INCOME'] + \
                             test_pivot['U-BOX OTHER INCOME'] + \
                             test_pivot['U-BOX DELIVERY INCOME']

test_pivot['NET SALES'] = test_pivot['SALES'] - test_pivot['COST OF SALES']

#TODO: Remove space from 'THIRD PARTY LEASE '
test_pivot['TOTAL REVENUE'] = test_pivot['STORAGE INCOME'] + \
                             test_pivot['NET SALES'] + \
                             test_pivot['MISCELLANEOUS INCOME'] + \
                             test_pivot['U-BOX INCOME'] + \
                             test_pivot['U-MOVE NET COMMISSION'] + \
                             test_pivot['INTERCOMPANY RENT'] + \
                             test_pivot['THIRD PARTY LEASE ']

test_pivot['TOTAL OPERATING EXPENSE'] = test_pivot['PERSONNEL'] + \
                                        test_pivot['REPAIRS AND MAINTENANCE/GENERAL'] + \
                                        test_pivot['UTILITIES'] + \
                                        test_pivot['TELEPHONE'] + \
                                        test_pivot['ADVERTISING'] + \
                                        test_pivot['SUPPLIES'] + \
                                        test_pivot['RENT-EQUIPMENT/LAND AND BLDGS'] + \
                                        test_pivot['LIABILITY INSURANCE'] + \
                                        test_pivot['PROPERTY TAX'] + \
                                        test_pivot['BAD DEBT EXPENSE'] + \
                                        test_pivot['OTHER OPERATING EXPENSE']

test_pivot['NET OPERATING INCOME'] = test_pivot['TOTAL REVENUE'] - test_pivot['TOTAL OPERATING EXPENSE']

# Income Statement Order ----
income_statement_order = ['STORAGE INCOME',
                          'NET SALES',
                          'MISCELLANEOUS INCOME',
                          'U-BOX INCOME',
                          'U-MOVE NET COMMISSION',
                          'INTERCOMPANY RENT',
                          'THIRD PARTY LEASE ',
                          'TOTAL REVENUE',
                          'PERSONNEL',
                          'REPAIRS AND MAINTENANCE/GENERAL',
                          'UTILITIES',
                          'TELEPHONE',
                          'ADVERTISING',
                          'SUPPLIES',
                          'RENT-EQUIPMENT/LAND AND BLDGS',
                          'LIABILITY INSURANCE',
                          'PROPERTY TAX',
                          'BAD DEBT EXPENSE',
                          'OTHER OPERATING EXPENSE',
                          'TOTAL OPERATING EXPENSE',
                          'NET OPERATING INCOME']

income_statement_df = test_pivot.loc[:, income_statement_order]

# Remove NOI = 0 rows ----
data_presence_mask = income_statement_df['NET OPERATING INCOME'] != 0
income_statement_df = income_statement_df[data_presence_mask]
income_statement_df.reset_index(inplace=True)

# Tidy version of income statement ----
income_statement_df = income_statement_df.melt(id_vars=['Date'])

