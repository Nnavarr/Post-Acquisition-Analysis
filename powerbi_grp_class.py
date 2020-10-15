import pandas as pd

# create grp_container ----
grp_names = [
    "F15_Q4",
    "F16_Q1",
    "F16_Q2",
    "F16_Q3",
    "F16_Q4",
    "F17_Q1",
    "F17_Q2",
    "F17_Q3",
    "F17_Q4",
    "F18_Q1",
    "F18_Q2",
    "F18_Q3",
    "F18_Q4",
    "F19_Q1",
    "F19_Q2",
    "F19_Q3",
    "F19_Q4",
    "F20_Q1",
    "F20_Q2",
    "F20_Q3",
    "F20_Q4",
]

# Group Numbers ----
grp_num_range = range(1, len(grp_names) + 1)
grp_num = []
for i in grp_num_range:
    grp_num.append(i)

# apply classification dict ----
classification_dict = dict(zip(grp_num, grp_names))
dataset['Group_Name'] = dataset.Group.map(classification_dict)
