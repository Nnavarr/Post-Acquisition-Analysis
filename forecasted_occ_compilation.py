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

"""
Creation: 4/21/2020
Author: Noe Navarro
SQL DB: Storage
Table: [FINANALYSIS].[dbo].[Storage_Forecast]

"""


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


def forecast_data_aggregation():

    """
    The function will compile the forecasted DF data into a single dataframe
    """

    # forecasted data import
    engine = create_connection(database="FINANALYSIS")
    query = dedent(
        """

        SELECT *
        FROM [FINANALYSIS].[dbo].[Storage_Forecast]

    """
    )
    forecast_df = pd.read_sql_query(query, engine)
    engine.close()

    # current date & wss import
    cur_month = date.today().replace(day=1)
    cur_month = cur_month - pd.DateOffset(months=1)
    cur_month = datetime.date.strftime(cur_month, format="%Y-%m-%d")

    engine = create_connection(database="Storage")
    query = dedent(
        """

        SELECT

        	sub.Date,
        	sub.MEntity,
        	sub.unm_product [product],
        	sub.unm_climate [climate],
        	ROUND(AVG(sub.unm_rentrate),2) [rent rate],
        	ROUND(SUM(sub.unm_numunits),2) [total units],
        	ROUND(SUM(sub.unm_occunits),2) [occ units],
        	ROUND(SUM(sub.unm_occunits)/SUM(sub.unm_numunits) ,2) [occ %]


        FROM

        	(
        	SELECT [ID]
        		  ,[MEntity]
        		  ,[unm_entity]
        		  ,[unm_pk]
        		  ,[unm_entfk]
        		  ,[unm_length]
        		  ,[unm_width]
        		  ,[unm_height]
        		  ,[unm_product]
        		  ,[unm_floor]
        		  ,[unm_elevation]
        		  ,[unm_climate]
        		  ,[unm_sqft]
        		  ,[unm_rentrate]
        		  ,[unm_rentpsqft]
        		  ,[unm_totsqft]
        		  ,[unm_occsqft]
        		  ,[unm_bonus]
        		  ,[unm_numunits]
        		  ,[unm_occunits]
        		  ,[unm_damunits]
        		  ,[unm_movein]
        		  ,[unm_moveout]
        		  ,[unm_sysuse]
        		  ,[unm_rsvdunits]
        		  ,[unm_vacunits]
        		  ,[unm_persqft]
        		  ,[unm_peroccunit]
        		  ,[unm_grospot]
        		  ,[unm_occrent]
        		  ,[unm_delinquent]
        		  ,[unm_timestamp]
        		  ,[unm_mco]
        		  ,[unm_district]
        		  ,[unm_orgfk]
        		  ,[unm_PendingReservations]
        		  ,[unm_DelinquencyStep1]
        		  ,[unm_DelinquencyStep2]
        		  ,[unm_DelinquencyStep3]
        		  ,[unm_DelinquencyStep4]
        		  ,[unm_access]
        		  ,[unm_streetrate]
        		  ,[Batch_Id]
        		  ,[Date]
        	  FROM [Storage].[dbo].[WSS_UnitMixUHI_Monthly_Archive]
        	  WHERE Date >= '2014-04-01'
        	  AND unm_product != 'UBOX'
              AND [MEntity] in {}
              AND [Date] = '{}'
        	  ) as sub

        GROUP BY sub.Date, sub.MEntity, sub.unm_product, sub.unm_climate
        ORDER BY sub.Date ASC

    """
    ).format(tuple(forecast_df.MEntity.unique()), cur_month)
    wss_df = pd.read_sql_query(query, engine)
    engine.close()

    # create minimum forecast date & filter forecast df
    max_wss_date = wss_df.Date.max()
    f_mask = forecast_df.FC_Date > max_wss_date
    forecast_df = forecast_df[f_mask]

    # concat forecasted DF on WSS occ DF
    merge_df = pd.concat([wss_df, forecast_df], axis=0)
    merge_df.Date = pd.to_datetime(merge_df.Date, format="%Y-%m-%d")
    merge_df.FC_Date = pd.to_datetime(merge_df.FC_Date, format="%Y-%m-%d")

    return merge_df


def sql_upload(df):

    # get user id
    user = os.environ.get("sql_user")
    if len(user) <= 7:
        user = "1217543"

    # create base connection
    base_con = (
        "Driver={{ODBC DRIVER 17 for SQL Server}};"
        "Server=OPSReport02.uhaul.amerco.org;"
        "Database=DEVTEST;"
        "UID={};"
        "PWD={};"
    ).format(user, os.environ.get("sql_pwd"))

    # URLLib finds the important information from our base connection
    params = urllib.parse.quote_plus(base_con)

    # SQLAlchemy takes all this info to create the engine
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    # upload to SQL
    df.to_sql("Quarterly_Acquisitions_F_Occ", engine, index=False, if_exists="replace")


"""
Run from command line
"""

if __name__ == "__main__":

    data_df = forecast_data_aggregation()
    print(
        f"The data has been aggregated. There are {data_df.shape[0]} rows and {data_df.shape[1]} columns."
    )
    sql_upload(data_df)
    print("The SQL upload is complete. Previous data was overwritten.")
