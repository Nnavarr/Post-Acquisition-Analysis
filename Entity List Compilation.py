import pandas as pd
import pyodbc
import numpy as np

from getpass import getpass, getuser
from wand.image import Image as Img


# SQL DB Connection ----
# UID = getuser()
# PWD = getpass(prompt= 'Type password')

# %s for position of UID and PWD Request  %(UID,PWD)) @ End after the string

# SQL Database Engines ----
mentity_engine = pyodbc.connect('DRIVER={SQL SERVER}; SERVER=OPSREPORT02.UHAUL.AMERCO.ORG;DATABASE=MEntity;UID'
                                '=1217543;PWD=Noe543N')
finanalysis_engine = pyodbc.connect('DRIVER={SQL SERVER}; SERVER=OPSREPORT02.UHAUL.AMERCO.ORG;DATABASE=FINANALYSIS'
                                    ';UID=1217543;PWD=Noe543N')


# STEP 1: Import Previous ----
# Import Existing Quarterly Acquisitions List ----
existing_list = pd.read_csv("Z://group/MIA/Noe/Projects/Same Store & Cap Ex/Report/Quarterly Acquisitions/Master List.csv")
existing_mentity = existing_list["MEntity"]  # Extract MEntity column
existing_mentity.nunique()
# 311 Unique MEntity Numbers


# STEP 2: Filter DLR01 With Existing MEntity numbers ----
dlr01_query_initial = "SELECT * FROM ENTITY_DLR01 WHERE MEntity in {} AND [STATUS] = 'O' ORDER BY [ID] ASC, [MEntity]".format(tuple(existing_mentity))
dlr01_initial = pd.read_sql_query(dlr01_query_initial, mentity_engine)
dlr01_unique_mentity = dlr01_initial.MEntity.unique()


# STEP 3: Determine if the center is still open ----
mentity_check = np.average((np.isin(existing_mentity, dlr01_unique_mentity)))
# A11 MEntity numbers are still open, count = 311 (matches line 33)

# Logical check for equivalency ----
if mentity_check < 1:
  print("A center within the Existing Acquisitions list has closed")
  closed_mentity = existing_list[-existing_list.MEntity.isin(dlr01_unique_mentity)]

else:
  print("All Existing Acquisitions are accounted for, proceed to new quarter group")


# Maintain Master Acquisitions File ----
writer = pd.ExcelWriter(
    'Z:/group/MIA/Noe/Projects/Same Store & Cap Ex/Report/Quarterly Acquisitions/Acquisition List Summary.xlsx',
    engine='xlsxwriter')

existing_list.to_excel(writer, sheet_name = 'Included Acquisitions')
writer.save()


# Import New Acquisitions List ----





