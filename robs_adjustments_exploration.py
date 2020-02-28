import numpy as np
import pandas as pd
from openpyxl import load_workbook

"""
All AREC Adjustments

The initial process development will reference a copy of the adjustments spreadsheet. Once fully developed, we can 
update the reference.

Author: Noe Navarro
Date: 10/28/2019

"""

#%% Step 1: Initial Data Import ----

# Initial data upload ----
wb = load_workbook(r'\\adfs01.uhi.amerco\users\1217543\robs_adjustments\All AREC Adjustments 2012-FY2019.xlsm',
                   data_only=True)
adjustments_sheet = wb['Adjustments']
adj_df = pd.DataFrame(adjustments_sheet.values)

# Initial filtering of DF ----
columns = adj_df.iloc[3, :]
adj_df.columns = columns

# Rename Columns ----
adj_df.rename(columns={'IS Date': 'IS_date',
                       'Storage Split': 'storage_split',
                       'Storage Adjustment': 'storage_adj',
                       'U-Move Adjustment': 'umove_adj',
                       'Line Item': 'line_item',
                       'IS Date ': 'is_date',
                       'Final Month': 'final_month',
                       'Final Adjustment': 'final_adj'}, inplace=True)

adj_df = adj_df.iloc[4:, :]
f_columns = adj_df.columns[~adj_df.columns.isna()]
adj_df = adj_df.loc[:, f_columns]
adj_df.reset_index(drop=True, inplace=True)

# Remove "None" & x from the "Center" column ----
adjustment_filter1 = (~pd.isna(adj_df['Center'])) & (adj_df['Center'] != 'x')
adj_df = adj_df[adjustment_filter1]

# Update Column data types ----
to_datetime = ['is_date', 'final_month']
to_float = ['Amount', 'final_adj', 'storage_split', 'storage_adj', 'umove_adj']

for col in to_datetime:
    adj_df[col] = pd.to_datetime(adj_df[col])

for col in to_float:
    adj_df[col] = adj_df[col].astype(float)

# Remove excess line_item labels ----
line_item_col = adj_df['line_item']
line_item_col = line_item_col.iloc[:, 0]

# Find and keep only first instance of "line_item"
line_item_bool = ~adj_df.columns.str.contains('line_item')

final_col_filter = []
for i in line_item_bool:
    count = 0
    if i == True:
        final_col_filter.append(i)
    elif (i == False) & (count == 0):
        bool_update = True
        final_col_filter.append(bool_update)
        count += 1
    else:
        final_col_filter.append(i)

