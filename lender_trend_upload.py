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

TMP_FILENAME = "tmp_Lender_Trend_Extract.csv"
################################################
# LOGGER
##################################################
# Connect to the logfile for this process
logger = logging.getLogger("lender_data_uploader.trends")
# logger.setLevel(logging.DEBUG)
# # File handler
# debugger = logging.FileHandler(
#     r"\\adfs01.uhi.amerco\departments\mia\group\MIA\DB\Automation"
#     r"\Python\Logfiles\Lender_Trends.log",
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
    query = "SELECT MAX([Date]) FROM Lender_Financing_Trends"
    target_date = cursor.execute(query).fetchone()[0]
    return target_date


def get_data(filepath, target_date=None):
    # Load in the historical data first
    df = pd.read_excel(filepath, sheet_name="Data", header=3)
    new_colnames = {
        "SAP":"SAP_Number",
        "Account_Number":"Account_Number",
        "Description":"Account_Description",
        "Line Item":"Line_Item"}
    keep_columns = list(new_colnames)
    keep_dates = [item for item in df.columns[8:] if type(item) == dt.datetime]
    # Filter for only items on or after target_date
    keep_dates = [item for item in keep_dates if item.date() > target_date]
    keep_columns.extend(keep_dates)

    # Clean up the data
    df = df[keep_columns].copy()
    df.dropna(subset=["SAP"], inplace=True)
    df["SAP"] = df["SAP"].astype(str)
    df["SAP"] = df["SAP"].apply(lambda x: x.split(".")[0])
    df.replace("x", 0, inplace=True)
    df = df[~df.Account_Number.isna()] # remove null account numbers
    df["Account_Number"] = df["Account_Number"].astype(int).astype(str)

    # Reshape and limit date range
    df = df.melt(id_vars=keep_columns[:4], var_name="Date", value_name="Amount")
    df.rename(new_colnames, axis=1, inplace=True)
    df["Amount"].fillna(0, inplace=True)

    # Push to CSV to prevent memory issues
    df.to_csv(TMP_FILENAME, index=False)


def upload_data(target_date):
    """Uploads data to SQL from tmp csv file. If target_date is prior to the max
    data date in SQL, clears overlapping records before uploading"""
    test_date = get_max_date()
    if target_date < test_date:
        query = """DELETE FROM Lender_Financing_Trends
            WHERE [Date] > '{}'""".format(target_date)
        cursor.execute(query)
        con.commit()
    if os.path.exists(TMP_FILENAME):
        chunks = pd.read_csv("tmp_Lender_Trend_Extract.csv", chunksize = 50000)
        for df in chunks:
            df.to_sql("Lender_Financing_Trends", engine, index=False, if_exists="append")
        os.remove(TMP_FILENAME)
    else:
        logger.error(f"Processed data not found at {TMP_FILENAME}!")
        raise FileNotFoundError(f"Temporary file expected at {TMP_FILENAME} not found")


def main(date=None):
    filepath = (
        r"\\adfs01.uhi.amerco\departments\mia\group\MIA/Lender Financing Trends/"
        "Combined Trends of all Storage & U-Move Locations Managed by U-Haul/"
        "AREC Portfolio Trends/ALL AREC Consolidated IS Data.xlsm"
    )
    logger.debug("Starting job...")
    if not date:
        target_date = get_max_date()
    else:
        target_date = date
    logger.debug("Collecting records after {}".format(target_date))

    get_data(filepath, target_date)
    # logger.debug("Uploading {} new records...".format(df.shape[0]))

    # Upload to SQL
    upload_data(target_date)

    logger.debug("Job complete!")


if __name__ == '__main__':
    main()
