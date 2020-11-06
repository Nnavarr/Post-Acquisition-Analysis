import pandas as pd
import numpy as np

import center_list_update
import center_list_maintenance
import IS_compilation
import forecasted_occ_compilation

"""
Author: Noe Navarro
Date: 11/5/2020
Objective: Run the entire new acquisitions process

Process in order
    1) Center list update
    2) Center list maintenance
    3) Income statement compilation
    4)

"""

# New acquisitions process
center_list_update.main()
center_list_maintenance.main()
IS_compilation.main()
forecasted_occ_compilation.main()
