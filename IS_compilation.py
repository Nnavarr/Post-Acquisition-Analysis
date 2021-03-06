import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
from datetime import datetime
from textwrap import dedent

from IS_function import income_statement
from sap_db_filter import chart_of_accounts, sap_db_query, create_connection

# SQL Upload Packages ----
import sqlalchemy, urllib

# create quarterly acquisitions grp
def grp_classification():

    #TODO: Create a dynamically generated grp classification

    # grp labels
    grp_names = [
        "F15_Q4",
        "F16_Q1", "F16_Q2", "F16_Q3", "F16_Q4",
        "F17_Q1", "F17_Q2", "F17_Q3", "F17_Q4",
        "F18_Q1", "F18_Q2", "F18_Q3", "F18_Q4",
        "F19_Q1", "F19_Q2", "F19_Q3", "F19_Q4",
        "F20_Q1", "F20_Q2", "F20_Q3", "F20_Q4",
        "F21_Q1", "F21_Q2", "F21_Q3", "F21_Q4",
        "F22_Q1", "F22_Q2", "F22_Q3", "F22_Q4",
        "F23_Q1", "F23_Q2", "F23_Q3", "F23_Q4"
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
    center_list_query = dedent("""
        SELECT
            *
        FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]
        WHERE
            Profit_Center IS NOT NULL
            AND [Include?] = 1
        """)

    center_df = pd.read_sql_query(center_list_query, engine)
    engine.close()

    # convert profit center to object
    center_df.Profit_Center = center_df.Profit_Center.astype(str).str[:-2]

    # assert statement to ensure no duplicate profit centers & return complete list
    assert (sum(center_df.duplicated(subset="Profit_Center")) == 0), "Duplicate Profit Centers Present, DO NO PROCEED"
    center_list = list(center_df.Profit_Center.unique())

    final_list = []
    for pc in center_list:
        if len(pc) > 3:
            final_list.append(pc)
            continue

    return center_df, final_list

def IS_compilation(center_df, center_list, classification_df):

    """
    Author: Noe Navarro
    Date: sometime 2019

    param1: center_df; pandas DF of new acquisitions
    param2: center_list; list of profit centers associated with included new acquisitions
        **Important**
        Included is defined as 'standalone' centers. That is NOT remote, abutting, non UHI owned (SAC)
    param3: classification_df; pandas dataframe of grp_name to group number (can be redeveloped as dictionary)
    return: aggregated pandas dataframe of each center and their associated income statement.
            This then gets uploaded into SQL
    """

    # calculate fiscal_year
    today = datetime.today()

    fy_list = []
    if today.month >= 4:
        fy_current = today.year + 1
        fy_last = fy_current - 1
    else:
        fy_current = today.year
        fy_last = fy_current - 1

    fy_list.extend([fy_last, fy_current])

    """
    Why include 2 years? Well, often the best check to ensure a 'standalone' center is to
    look at the trends (rev & expenses). A 2 year trend is included to ensure any new acquisitons
    are in fact, new. This part of the process remains somewhat manual with the opportunity to check
    individually before being tagged as "included".
    """

    # import sap data
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
    month_start = datetime.today().replace(day=1).date()
    max_date_mask = q_is_aggregate.date <= str(month_start)
    q_is_aggregate = q_is_aggregate[max_date_mask]

    # Import group name and number ----
    center_df.rename(columns={'Profit_Center': 'profit_center'}, inplace=True)

    aggregate_income_statement = pd.merge(
        left=q_is_aggregate,
        right=center_df.loc[:, ["profit_center", "Group", "MEntity"]],
        on="profit_center",
        how="left",
    )

    # create fiscal month and year calculation
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

    # append group name
    aggregate_income_statement = pd.merge(
        left=aggregate_income_statement, right=classification_df, how="left", on="Group"
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

    return aggregate_income_statement

def upload_is_sql(df):

    """
    Author: Noe Navarro
    Date: sometime 2019

    param1: df; pandas DF of center with their associated income statements. This is
                the output from the "IS_compilation" function.

    return: NA; Uploads SQL data and re-aggregates the past 2 years to check any new
                acquisitions for true 'standalone' status.
    """

    # establish SQL connection
    user = '1217543'
    base_con = (
        "Driver={{ODBC DRIVER 17 for SQL Server}};"
        "Server=OPSReport02.uhaul.amerco.org;"
        "Database=DEVTEST;"
        "UID={};"
        "PWD={};"
    ).format(user, os.environ.get("sql_pwd"))

    # URLLib finds the important information from our base connection
    params = urllib.parse.quote_plus(base_con)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    print("Uploading income statement data to SQL ...")

    # remove existing data (for previous fiscal years)
    del_query = dedent("""
        DELETE FROM  DEVTEST.dbo.Quarterly_Acquisitions_IS
        WHERE fiscal_year BETWEEN {} AND {}
    """).format("'" + str(fy_list[0]) + "'", "'" + str(fy_list[1]) + "'")

    # commit delete
    engine.execute(del_query)

    # uplaod new data
    df.to_sql('Quarterly_Acquisitions_IS', engine, index=False, if_exists='append')
    print(f"Data was uploaded successfully")

def main():
    try:
        grp_class = grp_classification()
        center_df, center_list = center_list_import()

        print("Starting income statement compilation...")
        income_statmenet_df = IS_compilation(center_df, center_list, grp_class)

        print("Income statement aggregation complete. Starting SQL Upload...")
        upload_is_sql(income_statmenet_df)
        print("The upload is complete. Quarterly acquisitions income statement data is up-to-date.")

    except:
        print("An error accured within the aggregation")

# calling from command line
if __name__ == '__main__':
    main()
