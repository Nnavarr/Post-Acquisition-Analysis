import pandas as pd
import numpy as np

# Import Data ----
adjustments_df = pd.read_excel(r'\\adfs01.uhi.amerco\departments\mia\group\MIA\Noe\Projects\Post Acquisition\Report\Quarterly Acquisitions\F19 Q4\Robs Adjustments\All_AREC_Adjustments.xlsm',
                               sheet_name='Adjustments',
                               header=3)

adjustments_f_df = adjustments_df[~pd.isna(adjustments_df['Grp_Num'])]
adjustments_f_df.


