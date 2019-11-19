import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime


"""
Test commmit


test
"""


from income_statement_compilation import income_statement
from sap_db_filter import chart_of_accounts, sap_db_query, create_connection

# SQL Upload Packages ----
import sqlalchemy, urllib

# -----------------------------
# Quarterly Acquisitions Format
# -----------------------------

# Group Names ----
# TODO: Update the name every new quarter
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
    "F20_Q4",
]

# Group Numbers ----
grp_num_range = range(1, len(grp_names) + 1)
grp_num = []
for i in grp_num_range:
    grp_num.append(i)

# Quarterly Acquisitions Classification DF ----
quarter_grp_class_df = pd.DataFrame(zip(grp_names, grp_num))
quarter_grp_class_df.rename(columns={0: "grp_name", 1: "Group"}, inplace=True)

# ---------------------------------------------------------------------------------------------------------------------
# Import Quarterly Acquisitions Center List
# Entity List ----
# entity_list = pd.read_excel(
#     r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\Master_Acquisitions_List.xlsx')


# Entity list F20 Q2 ----
entity_list = pd.read_excel(
    r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q2\acquisitions_list.xlsx"
)

# ----------------------------
# Data Processing: Entity List
# ----------------------------
entity_list["Profit_Center"] = entity_list["Profit_Center"].fillna(0).astype("int64")
entity_list["Profit_Center"] = entity_list["Profit_Center"].astype("object")
entity_list.rename(columns={"Profit_Center": "profit_center"}, inplace=True)
entity_list["profit_center"] = entity_list["profit_center"].astype(str)
entity_in = entity_list[entity_list["Include?"] == "Yes"]

# Duplicate Profit Center Check ----
assert (
    sum(entity_in.duplicated(subset="profit_center")) == 0
), "Duplicate Profit Centers Present, DO NO PROCEED"

# List of Profit Centers ----
quarter_acq_pc_list = list(entity_in["profit_center"].unique())

# -----------------------------
# Income Statement Compilation
# -----------------------------

# Filter SAP DB for relevant profit centers and account numbers ----
sap_engine = create_connection(database="SAP_Data")

# Compile individual fiscal year data ----
fy_list = [2015, 2016, 2017, 2018, 2019, 2020]
is_container = []

for i in fy_list:
    sap_db = sap_db_query(quarter_acq_pc_list, fiscal_yr=i)
    is_container.append(sap_db)

sap_db = pd.concat(is_container)

income_statement_container = map(
    lambda x: income_statement(
        profit_center=x, sap_data=sap_db, line_item_dict=chart_of_accounts
    ),
    quarter_acq_pc_list,
)

q_acq_is_list = [n for n in list(income_statement_container)]
income_statement_dict = dict(zip(quarter_acq_pc_list, q_acq_is_list))

# Concatenate into a single data frame for SQL upload ----
q_is_aggregate = pd.concat(income_statement_dict, ignore_index=True)

# Import group name and number ----
aggregate_income_statement = pd.merge(
    left=q_is_aggregate,
    right=entity_in.loc[:, ["profit_center", "Group", "MEntity"]],
    on="profit_center",
    how="left",
)

# Create Fiscal Year / Month column ----
aggregate_income_statement["Year"] = aggregate_income_statement["date"].apply(
    (lambda x: x.year)
)
aggregate_income_statement["Month"] = aggregate_income_statement["date"].apply(
    (lambda x: x.month)
)

aggregate_income_statement["fiscal_year"] = np.where(
    aggregate_income_statement["Month"] <= 3,
    aggregate_income_statement["Year"],
    aggregate_income_statement["Year"] + 1,
)

aggregate_income_statement["fiscal_month"] = np.where(
    aggregate_income_statement["Month"] >= 4,
    aggregate_income_statement["Month"] - 3,
    aggregate_income_statement["Month"] + 9,
)

# Append group name ----
aggregate_income_statement = pd.merge(
    left=aggregate_income_statement, right=quarter_grp_class_df, how="left", on="Group"
)

# Format Data for Upload ----
aggregate_income_statement = aggregate_income_statement.loc[
    :,
    [
        "date",
        "profit_center",
        "MEntity",
        "line_item",
        "value",
        "grp_name",
        "Group",
        "Month",
        "Year",
        "fiscal_year",
        "fiscal_month",
    ],
]
aggregate_income_statement.rename(
    columns={"Group": "grp_num", "Month": "month", "Year": "year"}, inplace=True
)

# ---------------
# Upload to SQL
# --------------
user = "1217543"

# Traditional SQL Connection protocol - this is still necessary
base_con = (
    "Driver={{ODBC DRIVER 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database=DEVTEST;"
    "UID={};"
    "PWD={};"
).format(user, os.environ.get("sql_pwd"))
con = pyodbc.connect(base_con)

# URLLib finds the important information from our base connection
params = urllib.parse.quote_plus(base_con)

# SQLAlchemy takes all this info to create the engine
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# -----------
# Upload Code
# -----------
# max_date = '2019-09-01'
# max_date_mask = aggregate_income_statement['date'] <= max_date
# aggregate_income_statement = aggregate_income_statement[max_date_mask]

# aggregate_income_statement.to_sql('Quarterly_Acquisitions_IS', engine, index=False, if_exists='replace')


# ------------------------------
# Aggregate by Quarterly Report
# ------------------------------

q_acq_db = create_connection(database="DEVTEST")
acq_query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_IS]"
acq_df = pd.read_sql_query(acq_query, q_acq_db)

# Aggregate IS ----
aggregate_is = acq_df.groupby(
    ["date", "line_item", "fiscal_year", "fiscal_month", "grp_name", "grp_num"]
)["value"].sum()
aggregate_df = aggregate_is.reset_index()
aggregate_df.head()

# Match Existing Titles ----
line_item_dict = {
    "NET SALES": "NET_SALES",
    "U-BOX INCOME": "U-BOX",
    "INTERCOMPANY RENT": "INTERCOMPANY LEASE",
    "TOTAL REVENUE": "REVENUE",
    "TOTAL OPERATING EXPENSE": "TOTAL_OP_EXPENSE",
    "NET OPERATING INCOME": "NOI",
}

# Update line item names ----
aggregate_df.line_item.replace(line_item_dict, inplace=True)

# # Export to CSV ----
# aggregate_df.to_csv(r'Z:\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\F20 Q2\Data\aggregate_is.csv',
#                     index=False)
# acq_df.groupby('grp_name')['MEntity'].nunique()
