import os
import logging
import sys

import lender_trend_upload
import lender_ubox_upload
import lender_adjustment_upload

################################################
# LOGGER
##################################################
# Connect to the logfile for this process
logger = logging.getLogger("lender_data_uploader")
logger.setLevel(logging.DEBUG)
# File handler
debugger = logging.FileHandler(
    r"\\adfs01.uhi.amerco\departments\mia\group\MIA\DB\Automation"
    r"\Python\Logfiles\Lender_Data_Uploader.log",
    mode="a+",
)
debugger.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
debugger.setFormatter(formatter)
# Add handlers to the logger
logger.addHandler(debugger)

lender_trend_upload.main()
lender_ubox_upload.main()
lender_adjustment_uploader.main()
