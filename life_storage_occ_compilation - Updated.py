import pandas as np
import pandas as pd
import pyodbc
import os
from getpass import getuser, getpass
import re
import datetime
from textwrap import dedent

import xlwings as xl

# SQL connection
user = '1217543'
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

# import life storage centers
ls_centers = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\2020\Post Acquisition\Quarterly Acquisitions\Script_Inputs\life_storage_samestore.xlsx')

ls_centers.profit_center = ls_centers.profit_center.astype(str)
exlude_centers = np.array(['7000007077', '7000006191', '7000006431'])
mask = ~ls_centers.profit_center.isin(exlude_centers)
ls_centers = ls_centers[mask]

# Import graph data
con = create_connection(database='Graph')
query = dedent("""

    SELECT
    	tot.Entity,
        tot.MEntity,
        tot.Date,
        tot.[Amount] as total,
    	graph.[Simple Owner]
    FROM [Graph].[dbo].[Tot] as tot
    LEFT JOIN [Graph].[dbo].[Index Match] as graph
    ON tot.MEntity = graph.MEntity
    WHERE  graph.MEntity in (
        -- filter for Cost centers present within the life storage same store
        SELECT
            MEntity
        FROM [Graph].[dbo].[Entity Info]
        WHERE [Cost Center] in {})""".format(tuple(ls_centers.profit_center)))
tot_rooms = pd.read_sql(query, con)

query = dedent("""

    SELECT
    	occ.Entity,
        occ.MEntity,
        occ.Date,
        occ.[Amount] as occ,
    	graph.[Simple Owner]
    FROM [Graph].[dbo].[Occ] as occ
    LEFT JOIN [Graph].[dbo].[Index Match] as graph
    ON occ.MEntity = graph.MEntity
    WHERE  graph.MEntity in (
        -- filter for Cost centers present within the life storage same store
        SELECT
            MEntity
        FROM [Graph].[dbo].[Entity Info]
        WHERE [Cost Center] in {})""".format(tuple(ls_centers.profit_center)))
occ_rooms = pd.read_sql(query, con)

# close connection
con.close()

# merge dataframes ----
ls_df = pd.merge(left=occ_rooms, right=tot_rooms.loc[:, ['MEntity', 'Date', 'total']], how='left', on=['MEntity', 'Date'])
ls_df['occupancy'] = ls_df.occ / ls_df.total
ls_df.fillna(0, inplace=True)

"""
Current Month
"""
# single month snapshot CY
month_date = pd.to_datetime('2020-09-01')
month_mask = ls_df.Date == month_date
month_df = ls_df[month_mask]
month_df.occupancy.median()

# CY Metrics
cy_m_occ = month_df.occ.sum() / month_df.total.sum() # mean occ (based on totals)
cy_m_median = month_df.occupancy.median() # median occ
cy_m_perc_above85 = sum(month_df.occupancy >= .85) / 537 # % above 85%

# bucket creation
buckets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

first = sum(month_df.occupancy <= .7749)
second = sum((month_df.occupancy >= .775) & (month_df.occupancy <= .7999))
third = sum((month_df.occupancy >= .80) & (month_df.occupancy <= .8249))
fourth = sum((month_df.occupancy >= .825) & (month_df.occupancy <= .8499))
fifth = sum((month_df.occupancy >= .85) & (month_df.occupancy <= .8749))
sixth = sum((month_df.occupancy >= .875) & (month_df.occupancy <= .8999))
seventh = sum((month_df.occupancy >= .90) & (month_df.occupancy <= .9249))
eight = sum((month_df.occupancy >= .925) & (month_df.occupancy <= .9499))
ninth = sum((month_df.occupancy >= .95) & (month_df.occupancy <= .9749))
tenth = sum((month_df.occupancy >= .975) & (month_df.occupancy <= 1))

bucket_count = [first, second, third, fourth, fifth, sixth, seventh, eight, ninth, tenth]
cy_m_dict = dict(zip(buckets, bucket_count))
cy_m_dict.values()
"""
Current Month LY
"""
month_date_ly = pd.to_datetime('2019-09-01')
month_mask_ly = ls_df.Date == month_date_ly
month_df_ly = ls_df[month_mask_ly]

month_df_ly.occ.sum() / month_df_ly.total.sum() # mean occ (based on totals)
month_df_ly.occupancy.median() # median occ
sum(month_df_ly.occupancy >= .85) / 537 # % above 85%

# bucket creation
first = sum(month_df_ly.occupancy <= .7749)
second = sum((month_df_ly.occupancy >= .775) & (month_df_ly.occupancy <= .7999))
third = sum((month_df_ly.occupancy >= .80) & (month_df_ly.occupancy <= .8249))
fourth = sum((month_df_ly.occupancy >= .825) & (month_df_ly.occupancy <= .8499))
fifth = sum((month_df_ly.occupancy >= .85) & (month_df_ly.occupancy <= .8749))
sixth = sum((month_df_ly.occupancy >= .875) & (month_df_ly.occupancy <= .8999))
seventh = sum((month_df_ly.occupancy >= .90) & (month_df_ly.occupancy <= .9249))
eighth = sum((month_df_ly.occupancy >= .925) & (month_df_ly.occupancy <= .9499))
ninth = sum((month_df_ly.occupancy >= .95) & (month_df_ly.occupancy <= .9749))
tenth = sum((month_df_ly.occupancy >= .975) & (month_df_ly.occupancy <= 1))

print(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth)

"""
Quarter Aggregation
"""
q_start = month_date.replace(month=7) # TTM
q_mask = (ls_df.Date >= q_start) & (ls_df.Date <= month_date)
q_df = ls_df[q_mask]

# CY Metrics
q_df.occ.sum() / q_df.total.sum() # mean occ (based on totals)
q_df_grp = q_df.groupby('MEntity')['occ', 'total'].sum().reset_index()
q_df_grp['occupancy'] = q_df_grp.occ / q_df_grp.total
q_df_grp.occupancy.median() # median occ
sum(q_df_grp.occupancy >= .85) / 537 # % above 85%


# bucket creation
first = sum(q_df_grp.occupancy <= .7749)
second = sum((q_df_grp.occupancy >= .775) & (q_df_grp.occupancy <= .7999))
third = sum((q_df_grp.occupancy >= .80) & (q_df_grp.occupancy <= .8249))
fourth = sum((q_df_grp.occupancy >= .825) & (q_df_grp.occupancy <= .8499))
fifth = sum((q_df_grp.occupancy >= .85) & (q_df_grp.occupancy <= .8749))
sixth = sum((q_df_grp.occupancy >= .875) & (q_df_grp.occupancy <= .8999))
seventh = sum((q_df_grp.occupancy >= .90) & (q_df_grp.occupancy <= .9249))
eighth = sum((q_df_grp.occupancy >= .925) & (q_df_grp.occupancy <= .9499))
ninth = sum((q_df_grp.occupancy >= .95) & (q_df_grp.occupancy <= .9749))
tenth = sum((q_df_grp.occupancy >= .975))

print(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth)

"""
Quarter Agg Last Year
"""
q_start_ly = month_date.replace(month=7, year=2019) # TTM
q_mask = (ls_df.Date >= q_start_ly) & (ls_df.Date <= month_date_ly)
q_df_ly = ls_df[q_mask]

# CY Metrics
q_df_ly.occ.sum() / q_df_ly.total.sum() # mean occ (based on totals)
q_df_ly_grp = q_df_ly.groupby('MEntity')['occ', 'total'].sum().reset_index()
q_df_ly_grp['occupancy'] = q_df_ly_grp.occ / q_df_ly_grp.total
q_df_ly_grp.occupancy.median() # median occ
sum(q_df_ly_grp.occupancy >= .85) / 537 # % above 85%

# bucket creation
first = sum(q_df_ly_grp.occupancy <= .7749)
second = sum((q_df_ly_grp.occupancy >= .775) & (q_df_ly_grp.occupancy <= .7999))
third = sum((q_df_ly_grp.occupancy >= .80) & (q_df_ly_grp.occupancy <= .8249))
fourth = sum((q_df_ly_grp.occupancy >= .825) & (q_df_ly_grp.occupancy <= .8499))
fifth = sum((q_df_ly_grp.occupancy >= .85) & (q_df_ly_grp.occupancy <= .8749))
sixth = sum((q_df_ly_grp.occupancy >= .875) & (q_df_ly_grp.occupancy <= .8999))
seventh = sum((q_df_ly_grp.occupancy >= .90) & (q_df_ly_grp.occupancy <= .9249))
eighth = sum((q_df_ly_grp.occupancy >= .925) & (q_df_ly_grp.occupancy <= .9499))
ninth = sum((q_df_ly_grp.occupancy >= .95) & (q_df_ly_grp.occupancy <= .9749))
tenth = sum((q_df_ly_grp.occupancy >= .975))

print(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth)



"""
TTM
"""
ttm_start = month_date.replace(month=10, year=2019) # TTM
ttm_mask = (ls_df.Date >= ttm_start) & (ls_df.Date <= month_date)
ttm_df = ls_df[ttm_mask]


# CY Metrics
ttm_df.occ.sum() / ttm_df.total.sum() # mean occ (based on totals)
ttm_df_grp = ttm_df.groupby('MEntity')['occ', 'total'].sum().reset_index()
ttm_df_grp['occupancy'] = ttm_df_grp.occ / ttm_df_grp.total
ttm_df_grp.occupancy.median() # median occ
sum(ttm_df_grp.occupancy >= .85) / 537 # % above 85%

sum(ttm_df_grp.occupancy > 0)

# bucket creation
first = sum(ttm_df_grp.occupancy <= .7749)
second = sum((ttm_df_grp.occupancy >= .775) & (ttm_df_grp.occupancy <= .7999))
third = sum((ttm_df_grp.occupancy >= .80) & (ttm_df_grp.occupancy <= .8249))
fourth = sum((ttm_df_grp.occupancy >= .825) & (ttm_df_grp.occupancy <= .8499))
fifth = sum((ttm_df_grp.occupancy >= .85) & (ttm_df_grp.occupancy <= .8749))
sixth = sum((ttm_df_grp.occupancy >= .875) & (ttm_df_grp.occupancy <= .8999))
seventh = sum((ttm_df_grp.occupancy >= .90) & (ttm_df_grp.occupancy <= .9249))
eighth = sum((ttm_df_grp.occupancy >= .925) & (ttm_df_grp.occupancy <= .9499))
ninth = sum((ttm_df_grp.occupancy >= .95) & (ttm_df_grp.occupancy <= .9749))
tenth = sum((ttm_df_grp.occupancy >= .975))

print(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth)


"""
TTM LY
"""
ttm_start_ly = ttm_start.replace(year=2018) # TTM
ttm_mask_ly = (ls_df.Date >= ttm_start_ly) & (ls_df.Date <= month_date_ly)
ttm_df_ly = ls_df[ttm_mask_ly]

# LY Metrics
ttm_df_ly.occ.sum() / ttm_df_ly.total.sum() # mean occ (based on totals)
ttm_df_grp_ly = ttm_df_ly.groupby('MEntity')['occ', 'total'].sum().reset_index()
ttm_df_grp_ly['occupancy'] = ttm_df_grp_ly.occ / ttm_df_grp_ly.total
ttm_df_grp_ly.occupancy.median() # median occ
sum(ttm_df_grp_ly.occupancy >= .85) / 537 # % above 85%

# bucket creation
first = sum(ttm_df_grp_ly.occupancy <= .7749)
second = sum((ttm_df_grp_ly.occupancy >= .775) & (ttm_df_grp_ly.occupancy <= .7999))
third = sum((ttm_df_grp_ly.occupancy >= .80) & (ttm_df_grp_ly.occupancy <= .8249))
fourth = sum((ttm_df_grp_ly.occupancy >= .825) & (ttm_df_grp_ly.occupancy <= .8499))
fifth = sum((ttm_df_grp_ly.occupancy >= .85) & (ttm_df_grp_ly.occupancy <= .8749))
sixth = sum((ttm_df_grp_ly.occupancy >= .875) & (ttm_df_grp_ly.occupancy <= .8999))
seventh = sum((ttm_df_grp_ly.occupancy >= .90) & (ttm_df_grp_ly.occupancy <= .9249))
eighth = sum((ttm_df_grp_ly.occupancy >= .925) & (ttm_df_grp_ly.occupancy <= .9499))
ninth = sum((ttm_df_grp_ly.occupancy >= .95) & (ttm_df_grp_ly.occupancy <= .9749))
tenth = sum((ttm_df_grp_ly.occupancy >= .975) & (ttm_df_grp_ly.occupancy <= 1))

print(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth)
