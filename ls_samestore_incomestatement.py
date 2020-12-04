import pandas as pd
import numpy as np
import xlwings as xw

import lender_data_uploader

"""
Date: 11/10/2020
Author: Noe Navarro
Objective: Master run file for same store income statement and occupancy reports

Update Log
----------
Version 0.1.0: Inception of life | 11/10/2020 | NN
"""

# establish workbook parameters
occ_wb = xw.Book()
is_wb = xw.Book()

def main():

    """
    Master function for compilation of life storage same store report.
    Before running, the appropriate MEntity numbers should already be identified.
    """
    try:

        # upload lender trend data
        lender_trend_upload.main()
        lender_ubox_upload.main()
        lender_adjustment_uploader.main()

        # compile excel report

        #TODO: solidify Excel spreadsheet output



    except:
        testing_update.trend()
