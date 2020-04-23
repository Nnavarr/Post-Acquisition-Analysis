import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re
import datetime
import sqlalchemy, urllib
from textwrap import dedent
from datetime import timedelta, date
import xlwings as xw

"""
Creation: 4/22/2020
Author: Noe Navarro
SQL DB: DEVTEST
Table: [DEVTEST].[dbo].[Quarterly_Acquisitions_IS]
Objective:
    Create an automated data import process for the quarterly acquisitons income
    statement view
"""

# SQL connection
def create_connection(database):
    # load password from env, entry if not available
    pwd = os.environ.get("sql_pwd")
    if pwd is None:
        pwd = getpass()

    # load user from env, user = 1217543 if not available
    user = os.environ.get("sql_user")
    if len(user) < 7:
        user = "1217543"

    # load user and create connection string
    cnxn_str = (
        r"Driver={{ODBC Driver 17 for SQL Server}};"
        r"Server=OPSReport02.uhaul.amerco.org;"
        r"Database=" + database + ";"
        r"UID={};PWD={};"
    ).format(user, pwd)

    # return connection object
    return pyodbc.connect(cnxn_str)


def is_data_import():
    # connect to db
    engine = create_connection(database="DEVTEST")
    query = "SELECT * FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_IS]"
    df = pd.read_sql(query, engine)
    engine.close()

    # calculate quarter, cal_month
    quarter_dict = {
        1: 4,
        2: 4,
        3: 4,
        4: 1,
        5: 1,
        6: 1,
        7: 2,
        8: 2,
        9: 2,
        10: 3,
        11: 3,
        12: 3,
    }

    # calculate required columns (match template output)
    df.grp_name = df.grp_name.apply(lambda x: x.replace("_", " "))
    df.grp_name = df.grp_name.apply(lambda x: x.replace("F", "FY"))
    df["cal_month"] = df.date.apply(lambda x: x.month)
    df["quarter"] = df.cal_month.map(quarter_dict)
    df["val_thousands"] = df.value / 1000

    # column format
    col_format = [
        "date",
        "line_item",
        "value",
        "fiscal_year",
        "fiscal_month",
        "quarter",
        "cal_month",
        "grp_name",
        "grp_num",
        "val_thousands",
    ]

    # reformat DF for export
    df = df[col_format]
    df.sort_values(["date", "line_item"], ascending=True, inplace=True)

    return df


# This function contains the output file location. Be sure to update when needed.
def export_excel(df):

    # connect to excel workbook
    wb = xw.Book(
        r"\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Spreadsheet Templates\IS_template.xlsx"
    )

    data_sheet = wb.sheets["Data"]
    data_sheet.range("A2:J1048576").value = ""  # clears entire data range
    data_sheet.range("A2").options(index=False, header=False).value = df  # export


"""
Run from command line
"""

if __name__ == "__main__":

    # import data
    data_df = is_data_import()
    print("Income statement data was imported successfully. Beginnning export...")

    # export data to template
    export_excel(data_df)
    print("The data was exported successfully.")
