import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime

from IS_function import income_statement
from sap_db_filter import chart_of_accounts, sap_db_query, create_connection

# SQL Upload Packages ----
import sqlalchemy, urllib

# create quarterly acquisitions grp
def grp_classification():

    # grp labels
    grp_names = [
        "F15_Q4",
        "F16_Q1", "F16_Q2", "F16_Q3", "F16_Q4",
        "F17_Q1", "F17_Q2", "F17_Q3", "F17_Q4",
        "F18_Q1", "F18_Q2", "F18_Q3", "F18_Q4",
        "F19_Q1", "F19_Q2", "F19_Q3", "F19_Q4",
        "F20_Q1", "F20_Q2", "F20_Q3", "F20_Q4",
        ]

    # Group Numbers
    grp_num_range = range(1, len(grp_names) + 1)
    grp_num = []

    for i in grp_num_range:
        grp_num.append(i)

    quarter_grp_class_df = pd.DataFrame(zip(grp_names, grp_num))
    quarter_grp_class_df.rename(columns={0: "grp_name", 1: "Group"}, inplace=True)

    return quarter_grp_class_df

# import quarterly acquisitions list
def center_list_import():

    # establish SQL connection, import, and close
    engine = create_connection(database='Devtest')
    center_list_query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]"
    center_list = pd.read_sql_query(center_list_query, engine)
    engine.close()

    # retain included centers
    include_mask = center_list['Include?'] == True
    center_list = center_list[include_mask].copy()

    # convert profit center to object
    center_list.Profit_Center = center_list.Profit_Center.astype(str).str[:-2]

    # assert statement to ensure no duplicate profit centers & return complete list
    assert (sum(center_list.duplicated(subset="Profit_Center")) == 0), "Duplicate Profit Centers Present, DO NO PROCEED"
    center_df = list(center_list.Profit_Center.unique())

    final_list = []
    for pc in center_df:
        if len(pc) > 3:
            final_list.append(pc)
            continue

    return center_df, final_list

def IS_compilation(center_df, center_list):

    # import sap data
    fy_list = [2015, 2016, 2017, 2018, 2019, 2020]
    is_container = []

    for i in fy_list:
        sap_db = sap_db_query(center_list, fiscal_yr=i)
        is_container.append(sap_db)

    sap_db = pd.concat(is_container)

    # apply income aggregation function
    income_statement_container = map(
        lambda x: income_statement(
            profit_center=x, sap_data=sap_db, line_item_dict=chart_of_accounts
        ),
        center_list,
    )

    # create dictionary of income statements
    q_acq_is_list = [n for n in list(income_statement_container)]
    income_statement_dict = dict(zip(center_list, q_acq_is_list))

    # Concatenate into a single data frame for SQL upload ----
    q_is_aggregate = pd.concat(income_statement_dict, ignore_index=True)
    max_date_mask = q_is_aggregate.date <= pd.to_datetime('2019-12-01')
    q_is_aggregate = q_is_aggregate[max_date_mask]

    # Import group name and number ----
    aggregate_income_statement = pd.merge(
        left=q_is_aggregate,
        right=center_df.loc[:, ["Profit_Center", "Group", "MEntity"]],
        on="profit_center",
        how="left",
    )


