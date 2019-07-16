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


# -------------------------
# Income Statement Function
# -------------------------

def income_statement(profit_center, sap_data, line_item_dict):

    """
    The function will compile an income statement based on a list of profit center numbers

    :param profit_center: Profit Center Dictionary
    :param sap_data: SAP SQL Connection (imported to Python)
    :param line_item_dict: Chart of Accounts Dictionary
    :return: Income Statement DF for individual profit center
    """

    # Step 1: Filter SAP DB  ----
    pc_mask = sap_data['Profit_Center'] == profit_center
    temp_sap = sap_data[pc_mask]

    # Chart of accounts into list ----
    coa_acct_list = [n.Account.astype(str) for n in line_item_dict.values()]

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

    # Process Income Statement & Calculate NOI ----
    line_item_melt = line_item_df

    line_item_pivot = line_item_melt.pivot(index='Date', columns='lineitem', values='value').fillna(0)

    # Missing Line Items ----
    missing_mask = np.isin(line_items, line_item_pivot.columns.values)
    missing_line = line_items[~missing_mask]

    # Create Place Holder Values for missing line items ----
    for i in missing_line:
        line_item_pivot[i] = 0.00
        continue

    # Remove Intercompany Rent ----
    line_item_pivot['INTERCOMPANY RENT'] = 0.00

    # Calculate U-Box Income ----
    line_item_pivot['U-BOX INCOME'] = line_item_pivot['U-BOX STORAGE INCOME'] + \
                                 line_item_pivot['U-BOX OTHER INCOME'] + \
                                 line_item_pivot['U-BOX DELIVERY INCOME']

    line_item_pivot['NET SALES'] = line_item_pivot['SALES'] - line_item_pivot['COST OF SALES']

    # TODO: Remove space from 'THIRD PARTY LEASE '
    line_item_pivot['TOTAL REVENUE'] = line_item_pivot['STORAGE INCOME'] + \
                                  line_item_pivot['NET SALES'] + \
                                  line_item_pivot['MISCELLANEOUS INCOME'] + \
                                  line_item_pivot['U-BOX INCOME'] + \
                                  line_item_pivot['U-MOVE NET COMMISSION'] + \
                                  line_item_pivot['INTERCOMPANY RENT'] + \
                                  line_item_pivot['THIRD PARTY LEASE ']

    line_item_pivot['TOTAL OPERATING EXPENSE'] = line_item_pivot['PERSONNEL'] + \
                                            line_item_pivot['REPAIRS AND MAINTENANCE/GENERAL'] + \
                                            line_item_pivot['UTILITIES'] + \
                                            line_item_pivot['TELEPHONE'] + \
                                            line_item_pivot['ADVERTISING'] + \
                                            line_item_pivot['SUPPLIES'] + \
                                            line_item_pivot['RENT-EQUIPMENT/LAND AND BLDGS'] + \
                                            line_item_pivot['LIABILITY INSURANCE'] + \
                                            line_item_pivot['PROPERTY TAX'] + \
                                            line_item_pivot['BAD DEBT EXPENSE'] + \
                                            line_item_pivot['OTHER OPERATING EXPENSE']

    line_item_pivot['NET OPERATING INCOME'] = line_item_pivot['TOTAL REVENUE'] - line_item_pivot['TOTAL OPERATING EXPENSE']

    # Income Statement Order ----
    income_statement_order = ['STORAGE INCOME',
                              'SALES',
                              'COST OF SALES',
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

    income_statement_df = line_item_pivot.loc[:, income_statement_order]

    # Remove NOI = 0 rows ----
    data_presence_mask = income_statement_df['NET OPERATING INCOME'] != 0
    income_statement_df = income_statement_df[data_presence_mask]
    income_statement_df.reset_index(inplace=True)

    # Tidy version of income statement ----
    income_statement_df = income_statement_df.melt(id_vars=['Date'])

    # Profit Center Classification ----
    income_statement_df['Profit_Center'] = profit_center

    # Re-arrange Columns ----
    income_statement_df = income_statement_df.loc[:, ['Date', 'Profit_Center', 'lineitem', 'value']]
    income_statement_df.rename(columns={'Date':'date',
                                        'lineitem':'line_item',
                                        'Profit_Center': 'profit_center'}, inplace=True)

    return(income_statement_df)

# ----------------------------------------------------------------------------------------------------------------------

# # Income Statement List ----
# profit_center_list = [str(n) for n in entity_list['Profit_Center'][0:9]]
#
# # Income Statement Compilation ----
# pc_income_statements = map(lambda x: income_statement(profit_center=x,
#                                  sap_data=sap_db,
#                                  line_item_dict=chart_of_accounts), profit_center_list)
#
# pc_income_statement_list = [n for n in list(pc_income_statements)]
# income_statement_dict = dict(zip(profit_center_list, pc_income_statement_list))
#
# test_df = income_statement_dict['7000010790']
#
# # Test PD Concat on Income Statements ----
# test_concat = pd.concat(income_dict.values())
# test_concat.reset_index(inplace=True, drop=True)

# Incorporate income statement compilation ----

test_pc = income_statement(profit_center='7000010790',
                           sap_data=sap_db,
                           line_item_dict=chart_of_accounts)

