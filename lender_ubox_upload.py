#####################################
# Created by Alex Reust
# Created on 2/19/2020
#######################################

import pandas as pd
import numpy as np
import os
from getpass import getuser, getpass
import platform
import pyodbc
import urllib
import sqlalchemy
import datetime as dt
import logging

################################################
# LOGGER
##################################################
# Connect to the logfile for this process
logger = logging.getLogger("lender_data_uploader.ubox")
# logger.setLevel(logging.DEBUG)
# # File handler
# debugger = logging.FileHandler(
#     r"\\adfs01.uhi.amerco\departments\mia\group\MIA\DB\Automation"
#     r"\Python\Logfiles\Lender_Trend_Ubox.log",
#     mode="a+",
# )
# debugger.setLevel(logging.DEBUG)
# # create formatter and add it to the handlers
# formatter = logging.Formatter(
#     "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# )
# debugger.setFormatter(formatter)
# # Add handlers to the logger
# logger.addHandler(debugger)

# SQL Connection
# Windows
if platform.system() == "Windows":
    if len(getuser()) < 7:
        user = '1217543'
    else:
        user = getuser()
    pw = os.environ.get("sql_pwd")
# Mac
elif platform.system() == "Darwin":
    user = os.environ.get("SQL_USER")
    pw = os.environ.get("SQL_PW")

base_con=(
    "Driver={{ODBC Driver 17 for SQL Server}};"
    "Server=OPSReport02.uhaul.amerco.org;"
    "Database={};"
    "UID={};"
    "PWD={};"
).format("SAP_Data", user, pw)
con = pyodbc.connect(base_con)
cursor = con.cursor()
# SQLAlchemy extension of standard connection
params = urllib.parse.quote_plus(base_con)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


def get_max_date():
    query = "SELECT MAX([Date]) FROM Lender_Financing_Ubox"
    target_date = cursor.execute(query).fetchone()[0]
    return target_date

def get_data(filepath, target_date=None):

    df = pd.read_excel(filepath, "U-Box Delivery OW vs Local", header=3)

    # Drop unwanted rows
    keep_account = ['U-BOX ONE-WAY DELIVERY INCOME', 'U-BOX LOCAL DELIVERY INCOME']
    df = df[df["Account"].isin(keep_account)]
    # Drop unwanted columns
    df.dropna(subset=["#"], inplace=True)
    df.drop(['x'], axis=1, inplace=True)
    df = df[list(df)[2:-5]].copy()

    # Clean the data
    df.rename({"#":"SAP_Number", "Account":"Account_Description"}, axis=1, inplace=True)
    df["SAP_Number"] = df["SAP_Number"].astype(str)
    df["SAP_Number"] = df["SAP_Number"].apply(lambda x: x.split(".")[0])
    df["Account_Number"] = 52152
    df.replace("x", 0, inplace=True)

    # Reshape the dataframe
    id_col_order = list(df)[0:2]
    id_col_order.insert(1, "Account_Number")
    df = df.melt(id_vars=id_col_order, var_name="Date", value_name="Amount")
    df["Amount"].fillna(0, inplace=True)

    df = df.query("Date > @target_date")

    return df


def upload_data(df, target_date):
    """Uploads data to SQL from tmp csv file. If target_date is prior to the max
    data date in SQL, clears overlapping records before uploading"""
    test_date = get_max_date()
    if target_date < test_date:
        query = """DELETE FROM Lender_Financing_Ubox
            WHERE [Date] > '{}'""".format(target_date)
        cursor.execute(query)
        con.commit()

    df.to_sql("Lender_Financing_Ubox", engine, if_exists="append", index=False)


def main(date=None):
    filepath = (
        r"\\adfs01.uhi.amerco\departments\mia\group\MIA/Lender Financing Trends/"
        "Combined Trends of all Storage & U-Move Locations Managed by U-Haul/"
        "AREC Portfolio Trends/All AREC U-Box Delivery OW-loc.xlsm"
    )
    logger.debug("Starting job...")
    if not date:
        target_date = get_max_date()
    else:
        target_date = date
    logger.debug("Collecting records after {}".format(target_date))

    df = get_data(filepath, target_date)
    logger.debug("Uploading {} new records...".format(df.shape[0]))

    # Upload to SQL if new records
    if df.shape[0] > 0:
        upload_data(df, target_date)

    logger.debug("Job complete!")


if __name__ == '__main__':
    main()
