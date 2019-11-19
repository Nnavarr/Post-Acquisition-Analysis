import pandas as pd
import numpy as np

from sap_db_filter import chart_of_accounts, line_items, sap_db_query
from income_statement_compilation import income_statement

"""
Repair and Maintenance Line Item Check
--------------------------------------
Objective:
    The objective of this script is to check the repair and maintenance line item
    to determine whether the monthly expense is within the normal bounds.

"""
repair_accounts = chart_of_accounts["REPAIRS AND MAINTENANCE/GENERAL"]
repair_accounts["Account"] = repair_accounts["Account"].astype(str)

# Profit Center Test ----
pc_list = ["7000007396", "7000006304"]

# Filter SAP DB ----
sap_db = sap_db_query(pc_list, fiscal_yr="2019")

# ------------------------------------------------------------------------------
# TODO: Develop process to compile monthly account totals and flag any outlier datapoints
# Filter SAP DB on relevant account numbers ----
account_filter = sap_db["Account"].isin(repair_accounts["Account"].unique())
sap_db = sap_db[account_filter]
sap_db.set_index("Date", inplace=True)

# Create individual DFs for each account number ----
unique_acct = repair_accounts["Account"].unique()

# Create individual DFs for account numbers ----
acct_container = []
for account in unique_acct:
    temp_filter = sap_db["Account"] == account
    temp_df = sap_db[temp_filter]

    # Append to list container ----
    acct_container.append(temp_df)

# Concatenate list of DF into a single DF ----
acct_df = pd.concat(acct_container)

"""
Volatility summary statistics
-----------------------------

Here we are interested in summarizing Monthly/Annual standard deviation amongst
the Repair and Maintenance line item, by individual Profit Center.

"""

# Group by profit center and account ----
grp_pc = acct_df.groupby(["Profit_Center", "Account"])

#


acct_df.info()
