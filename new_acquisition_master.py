import pandas as pd
import numpy as np
import center_list_update
import center_list_maintenance
import IS_compilation
import forecasted_occ_compilation
import logging
from textwrap import dedent

"""
Author: Noe Navarro
Date: 11/5/2020
Objective: Run the entire new acquisitions process

Process in order
    1) Center list update
        Imports new centers from the Real Estate Smartsheet (Parul Butala maintains this)

    2) Center list maintenance
        Updates missing and relevant values within the center acq list
            a. Entity name
            b. Loc Latitude
            c. Loc Longitude
            d. Simple Owner
            e. CBSA
            f. Profit Center

    3) Income statement compilation
        Compiles unadjusted income statement for relevant profit centers
        as determined by the center list update (step 1)

    4) Forecasted occupancy compilation
        Aggregates forecasted occupancy data that is run from a separate process.
        The process can be found here:
            \\adfs01.uhi.amerco\departments\mia\group\MIA\Alex\r_wdir\occ_ramp\Parabola V4.rmd
"""

# TODO: Ensure the following scripts are located within the UHI central directory
    # 1. Lender Trend Adjustments (needs to be integrated into the master .py after non-adjusted GL is complete)
    # 2. Storage Forecast Process

# log configuration
log_file = './new_acq.log'
logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.DEBUG,
    format="%(asctime)-15s %(levelname)-8s %(message)s")
logging.getLogger('prso.cache').disabled=True
logging.getLogger('parso.chace.picke').disabled=True

logger = logging.getLogger('new_acq_logger')

# Creat wrapper
def log_wrap(pre, post):
    #Wrapper
    def decorate(func):
        #decorator
        def call(*args, **kwargs):
            #actual wrapping
            pre(func)
            results = func(*args, **kwargs)
            post(func)
            return results
        return call
    return decorate

def entering(func):
    # pre function logging
    logger.debug('Entered %s', func.__name__)

def exiting(func):
    # pre function logging
    logger.debug('Entered %s', func.__name__)

#TODO: Update logger

@log_wrap(entering, exiting)
def main():

    # track processes that ran successfully
    success_list = []
    all_process_list = [center_list_update.__name__, center_list_maintenance.__name__, IS_compilation.__name__, forecasted_occ_compilation.__name__]

    try:
        # Run through each process
        center_list_update.main()
        success_list.append(center_list_update.__name__)

        center_list_maintenance.main()
        success_list.append(center_list_maintenance.__name__)

        IS_compilation.main()
        success_list.append(IS_compilation.__name__)

        forecasted_occ_compilation.main()
        success_list.append(forecasted_occ_compilation.__name__)

        #TODO: incorporate lender trend upload here.
        print('The new acquisitions process is now complete')

    except:
        failed_list = []
        for proc in all_process_list:
            if proc not in success_list:
                failed_list.append(proc)

        print(dedent("""
        There was an error in the process. The following did not run successfully: \n
        {} """).format(tuple(failed_list)))

# run from command line
if __name__ == '__main__':
    main()
