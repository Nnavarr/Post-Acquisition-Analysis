import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime

from sap_db_filter import line_items

# ##### Test Environment for Walker to SAP conversion ----
# account_df = pd.read_csv(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\walker_to_sap_dict.csv')
# walker_list = list(account_df.walker_acct)
# sap_list = list(account_df.sap_account)
# account_conversion_dict = dict(zip(walker_list, sap_list))
#

"""
Income Statement Function 
"""
def income_statement(profit_center, sap_data, line_item_dict, lender_reporting=False):

    """
    The function will compile an income statement based on a list of profit center numbers

    :param profit_center: Profit Center Dictionary
    :param sap_data: SAP SQL Connection (imported to Python)
    :param line_item_dict: Chart of Accounts Dictionary
    :return: Income Statement DF for individual profit center
    """

    # Check for lender reporting ----
    if lender_reporting is True:
        sap_data.rename(columns={'SAP': 'Profit_Center',
                                 'Account_Number': 'Account',
                                 'Line Item': 'lineitem',
                                 'Amount': 'value'},
                        inplace=True)

        # convert walker accounts to SAP ----
        account_df = pd.read_csv(
            r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\walker_to_sap_dict.csv')
        walker_list = list(account_df.walker_acct)
        sap_list = list(account_df.sap_account)
        account_conversion_dict = dict(zip(walker_list, sap_list))
        sap_data.Account = sap_data.Account.map(account_conversion_dict)

    # Step 1: Filter SAP DB  ----
    pc_mask = sap_data["Profit_Center"] == profit_center
    temp_sap = sap_data[pc_mask]

    # Chart of accounts into list ----
    if type(line_item_dict) == dict:
        coa_acct_list = [n.Account.astype(str) for n in line_item_dict.values()]
    else:
        coa_acct_list = list(line_item_dict["Account"])

    # Individual Line Item DF Compilation ----
    line_item_db = []
    for i, name in zip(coa_acct_list, line_items):

        # Filter SAP Data for individual line item DF ----
        temp_data = temp_sap[temp_sap["Account"].isin(i)]

        # Aggregate by Date and Account ----
        temp_data = temp_data.groupby(["Date"])["value"].agg("sum")
        temp_data = pd.DataFrame(temp_data)
        temp_data.reset_index(inplace=True)

        # Create a line item name column ----
        temp_data["lineitem"] = name

        # Append to Container ----
        line_item_db.append(temp_data)

        continue

    # Flatten list of individual line items ----
    line_item_df = pd.concat(line_item_db)
    line_item_df.reset_index(inplace=True, drop=True)

    # SAP Sign Flip due to presentation format ----
    line_item_df["value"] = np.where(
        line_item_df["lineitem"].isin(line_items[0:9]),
        line_item_df["value"] * -1,
        line_item_df["value"],
    )

    # Process Income Statement & Calculate NOI ----
    line_item_melt = line_item_df

    line_item_pivot = line_item_melt.pivot(
        index="Date", columns="lineitem", values="value"
    ).fillna(0)

    # Missing Line Items ----
    missing_mask = np.isin(line_items, line_item_pivot.columns.values)
    missing_line = line_items[~missing_mask]

    # Create Place Holder Values for missing line items ----
    for i in missing_line:
        line_item_pivot[i] = 0.00
        continue

    # Remove Intercompany Rent ----
    line_item_pivot["INTERCOMPANY RENT"] = 0.00

    # Calculate U-Box Income ----
    line_item_pivot["U-BOX INCOME"] = (
        line_item_pivot["U-BOX STORAGE INCOME"]
        + line_item_pivot["U-BOX OTHER INCOME"]
        + line_item_pivot["U-BOX DELIVERY INCOME"]
    )

    line_item_pivot["NET SALES"] = (
        line_item_pivot["SALES"] - line_item_pivot["COST OF SALES"]
    )

    # TODO: Remove space from 'THIRD PARTY LEASE '
    line_item_pivot["TOTAL REVENUE"] = (
        line_item_pivot["STORAGE INCOME"]
        + line_item_pivot["NET SALES"]
        + line_item_pivot["MISCELLANEOUS INCOME"]
        + line_item_pivot["U-BOX INCOME"]
        + line_item_pivot["U-MOVE NET COMMISSION"]
        + line_item_pivot["INTERCOMPANY RENT"]
        + line_item_pivot["THIRD PARTY LEASE "]
    )

    line_item_pivot["TOTAL OPERATING EXPENSE"] = (
        line_item_pivot["PERSONNEL"]
        + line_item_pivot["REPAIRS AND MAINTENANCE/GENERAL"]
        + line_item_pivot["UTILITIES"]
        + line_item_pivot["TELEPHONE"]
        + line_item_pivot["ADVERTISING"]
        + line_item_pivot["SUPPLIES"]
        + line_item_pivot["RENT-EQUIPMENT/LAND AND BLDGS"]
        + line_item_pivot["LIABILITY INSURANCE"]
        + line_item_pivot["PROPERTY TAX"]
        + line_item_pivot["BAD DEBT EXPENSE"]
        + line_item_pivot["OTHER OPERATING EXPENSE"]
    )

    line_item_pivot["NET OPERATING INCOME"] = (
        line_item_pivot["TOTAL REVENUE"] - line_item_pivot["TOTAL OPERATING EXPENSE"]
    )

    # Income Statement Order ----
    # TODO: Update U-Move to show Gross and U-Box to show Delivery Income ----
    income_statement_order = [
        "STORAGE INCOME",
        "SALES",
        "COST OF SALES",
        "NET SALES",
        "MISCELLANEOUS INCOME",
        "U-BOX INCOME",
        "U-BOX DELIVERY INCOME",
        "U-MOVE NET COMMISSION",
        "INTERCOMPANY RENT",
        "THIRD PARTY LEASE ",
        "TOTAL REVENUE",
        "PERSONNEL",
        "REPAIRS AND MAINTENANCE/GENERAL",
        "UTILITIES",
        "TELEPHONE",
        "ADVERTISING",
        "SUPPLIES",
        "RENT-EQUIPMENT/LAND AND BLDGS",
        "LIABILITY INSURANCE",
        "PROPERTY TAX",
        "BAD DEBT EXPENSE",
        "OTHER OPERATING EXPENSE",
        "TOTAL OPERATING EXPENSE",
        "NET OPERATING INCOME",
    ]

    income_statement_df = line_item_pivot.loc[:, income_statement_order]

    # Remove NOI = 0 rows ----
    data_presence_mask = income_statement_df["NET OPERATING INCOME"] != 0
    income_statement_df = income_statement_df[data_presence_mask]
    income_statement_df.reset_index(inplace=True)

    # Tidy version of income statement ----
    income_statement_df = income_statement_df.melt(id_vars=["Date"])

    # Profit Center Classification ----
    income_statement_df["Profit_Center"] = profit_center

    # Re-arrange Columns ----
    income_statement_df = income_statement_df.loc[
        :, ["Date", "Profit_Center", "lineitem", "value"]
    ]
    income_statement_df.rename(
        columns={
            "Date": "date",
            "lineitem": "line_item",
            "Profit_Center": "profit_center",
        },
        inplace=True,
    )

    return income_statement_df

# ----------------------------------------------------------------------------------------------------------------------
# Income Statement Use Example ----

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

# ----------------------------------------------------------------------------------------------------------------------
