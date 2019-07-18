import pandas as pd
import pyodbc
import numpy as np
import os
from getpass import getuser, getpass
import re

from Income_Statement_Compilation import income_statement
from SAP_DB_Filter import chart_of_accounts, sap_db_query, create_connection

# Import Quarterly Acquisitions Center List
# Entity List ----
entity_list = pd.read_excel(
    r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Quarterly Acquisitions\Script_Inputs\Master_Acquisitions_List.xlsx')


# ----------------------------
# Data Processing: Entity List
# ----------------------------
entity_list['Profit_Center'] = entity_list['Profit_Center'].fillna(0).astype('int64')
entity_list['Profit_Center'] = entity_list['Profit_Center'].astype('object')
entity_in = entity_list[entity_list['Include?'] == 'Yes']

# Duplicate Profit Center Check ----
assert sum(entity_in.duplicated(subset='Profit_Center')) == 0, 'Duplicate Profit Centers Present, DO NO PROCEED'

# List of Profit Centers ----
quarter_acq_pc_list = list(entity_in['Profit_Center'].unique().astype(str))

# -----------------------------
# Income Statement Compilation
# -----------------------------

# Filter SAP DB for relevant profit centers and account numbers ----
sap_engine = create_connection(database='SAP_Data')
sap_db = sap_db_query(quarter_acq_pc_list)

income_statement_container = map(lambda x: income_statement(profit_center=x,
                                  sap_data=sap_db,
                                  line_item_dict=chart_of_accounts), quarter_acq_pc_list)

q_acq_is_list = [n for n in list(income_statement_container)]
income_statement_dict = dict(zip(quarter_acq_pc_list, q_acq_is_list))

# Concatenate into a single data frame for SQL upload ----
q_is_aggregate = pd.concat(income_statement_dict, ignore_index=True)

# Checkpoint: Income Statement Compilation Complete
