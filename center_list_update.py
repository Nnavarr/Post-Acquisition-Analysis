import numpy as np
import pandas as pd
import pyodbc
import os
from getpass import getpass
import sqlalchemy, urllib

"""
The script can be called in order to import any new acquisitions that may have occured between the last run-time.
They will be uploaded into an existing SQL database.
"""

user = "1217543"

# group dictionary ----
grp_dict = {
    "2020-3": 20,
    "2020-4": 21,
    "2021-1": 22,
    "2021-2": 23,
    "2021-3": 24,
    "2021-4": 25,
    "2022-1": 26,
    "2022-2": 27,
    "2022-3": 28,
    "2022-4": 29,
}

# establish slq connection ----
def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user and create connection string
    cnxn_str = (
        r"Driver={{ODBC Driver 17 for SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)


# connect to existing smartsheet api ----
def smart_sheet_import():

    # establish connection
    engine = create_connection(database="RealEstateValuation")
    query = "SELECT * FROM [RealEstateValuation].[dbo].[Smartsheet_Closed]"
    df = pd.read_sql_query(query, engine)
    df.sort_values("Close of Escrow", inplace=True)

    # update appropriate columns to datetime
    cols = ["Close of Escrow", "Extract_Date"]
    for name in cols:
        df[name] = pd.to_datetime(df[name], format="%Y-%m-%d")

    # drop index column
    df.drop("index", axis=1, inplace=True)
    engine.close()

    return df


# connect to existing list ----
def quarter_acq_import():

    # establish connection
    engine = create_connection(database="DEVTEST")
    query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]"
    df = pd.read_sql_query(query, engine)
    df.sort_values("Close of Escrow", inplace=True)
    engine.close()

    return df


# extract new observations (based on close of escrow) ----
def new_acquisitions():

    # import current lists
    smartsheet = smart_sheet_import()
    q_acquisitions = quarter_acq_import()

    # establish new center list
    max_q_acq_date = q_acquisitions["Close of Escrow"].max()
    date_mask = smartsheet["Close of Escrow"] > max_q_acq_date
    new_acq = smartsheet[date_mask].copy()

    # group classification
    new_acq["acq_grp"] = (
        new_acq["FY Closed"].astype(str) + "-" + new_acq["Quarter Closed"].astype(str)
    )
    new_acq["Group"] = new_acq.acq_grp.map(grp_dict)
    new_acq.drop("acq_grp", axis=1, inplace=True)

    return new_acq


# data prep from individual sources ----
def dlr01_prep():

    # initial connection / import ----
    mentity_engine = create_connection(database="MEntity")
    dlr01_query = "SELECT * FROM [MEntity].[dbo].[ENTITY_DLR01] WHERE [MEntity] in {}".format(
        tuple(new_list.MEntity)
    )
    dlr01_df = pd.read_sql_query(dlr01_query, mentity_engine)
    dlr01_df = (
        dlr01_df.sort_values(["MEntity", "DATE_CLOSED"])
        .drop_duplicates("MEntity", keep="last")
        .copy()
    )
    dlr01_df.DATE_EXPIRATION = dlr01_df.DATE_EXPIRATION.astype(str)

    # error check ----
    assert dlr01_df.DATE_EXPIRATION.all() == "9999-12-31", print(
        "An center is not importing it's most recent Entity iteration"
    )
    assert dlr01_df.STATUS.all() == "O", print(
        "Not all centers are showing a status of 'O' for Open"
    )
    mentity_engine.close()

    # format dlr01 to match existing DF format ----
    dlr01_cols = ["MEntity", "ENTITY_NAME", "MCO_NUM"]

    # merge into new list acquisitions ----
    merge_df = pd.merge(
        left=new_list, right=dlr01_df[dlr01_cols], on="MEntity", how="left"
    )
    drop_cols = [
        # "Name",
        "FY Closed",
        "Quarter Closed",
        "Acres",
        "Improved SF",
        "Entity_Current",
        "Extract_Date",
    ]
    merge_df.drop(drop_cols, axis=1, inplace=True)

    return merge_df


def graph_prep(df):

    # initial connection / import ----
    mentity_engine = create_connection(database="MEntity")
    index_match_query = (
        "SELECT * FROM [Graph].[dbo].[Index Match] "
        "WHERE [MEntity] in {}".format(tuple(df.MEntity))
    )
    index_match_df = pd.read_sql_query(index_match_query, mentity_engine)
    index_match_df["Remote"] = index_match_df["Parent MEntity"].apply(
        lambda x: False if x is None else True
    )
    entity_info_query = (
        "SELECT * FROM [Graph].[dbo].[Entity Info] "
        "WHERE [MEntity] in {}".format(tuple(new_list.MEntity))
    )
    entity_info_df = pd.read_sql_query(entity_info_query, mentity_engine)
    entity_cols = ["MEntity", "CBSA", "Cost Center"]
    mentity_engine.close()

    # merge graph db & calculate include column ----
    graph_df = pd.merge(
        index_match_df, entity_info_df[entity_cols], on="MEntity", how="left"
    )
    merge_df = pd.merge(
        df, graph_df, on="MEntity", how="left", suffixes=["_newlist", "_graph"]
    )
    merge_df.Abutting = merge_df.Abutting.apply(lambda x: False if x is None else True)
    merge_df.Remote = merge_df.Remote.apply(lambda x: False if np.isnan(x) else x)
    merge_df["Include?"] = np.where(
        merge_df.Remote + merge_df.Abutting == 0, True, False
    )

    return merge_df


def real_add_prep(df):

    # initial connection / import ----
    rea_val_engine = create_connection(database="RealEstateValuation")
    real_additions_query = (
        "SELECT * FROM [RealEstateValuation].[dbo].[REV_REAL_ADDITIONS] "
        "WHERE [MEntity] in {}".format(tuple(new_list.MEntity))
    )
    real_add_df = pd.read_sql_query(real_additions_query, rea_val_engine)
    rea_val_engine.close()

    real_add_cols = ["MEntity", "Construction_Type"]
    merge_df = pd.merge(
        left=df, right=real_add_df[real_add_cols], on="MEntity", how="left"
    )

    # select appropriate columns ----
    existing_cols = [
        "Group",
        "Close of Escrow",
        "ENTITY_NAME",
        "District_newlist",
        "MCO_newlist",
        "Entity_newlist",
        "Cost Center",
        "Address",
        "City",
        "State_newlist",
        "Latitude",
        "Longitude",
        "CBSA",
        "Purchase Price",
        "Property Description",
        "Simple Owner",
        "Include?",
        "Construction_Type",
        "MEntity",
        "Parent MEntity",
        "Abutting",
        "Remote",
    ]
    merge_df = merge_df[existing_cols]
    merge_df["Purchase Price"] = merge_df["Purchase Price"].astype(int)

    return merge_df


def final_col_rename(df):

    df.rename(
        columns={
            "District_newlist": "DISTRICT_NO",
            "MCO_newlist": "MCO_NUM",
            "Entity_newlist": "Entity",
            "Cost Center": "Profit_Center",
            "State_newlist": "State",
            "Latitude": "LOC_LATITUDE",
            "Longitude": "LOC_LONGITUDE",
            "Construction_Type": "Type",
        },
        inplace=True,
    )
    return df


# update quarter acquisitions list ----
if __name__ == "__main__":

    try:
        new_list, q_acquisitions = new_acquisitions(), quarter_acq_import()
        df1 = dlr01_prep()
        df2 = graph_prep(df1)
        df3 = real_add_prep(df2)
        final_list = final_col_rename(df3)

        # upload to SQL ----
        base_con = (
            "Driver={{ODBC DRIVER 17 for SQL Server}};"
            "Server=OPSReport02.uhaul.amerco.org;"
            "Database=DEVTEST;"
            "UID={};"
            "PWD={};"
        ).format(user, os.environ.get("sql_pwd"))

        params = urllib.parse.quote_plus(base_con)
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        final_list.to_sql(
            "Quarterly_Acquisitions_List", engine, index=False, if_exists="append")
        # conn.close()
        engine.close()
        print(
            f"The quarterly acquisitions list has been updated successfully. \n There were {new_list.shape[0]} new acquisitions added."
        )

    except:
        print("There are no new entries within the smarthsheet acquisitions list")
