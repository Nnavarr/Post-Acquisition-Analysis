# Post-Acquisition-Analysis
----------------------------

#### Objective: Quarterly Aquisitions Dashboard which compiles Income Statement and Occupancy metrics for the property portfolio.
#### Scope: AREC owned properties
#### Frequency: Quarterly ; transitioning to monthly
#### Output: Excel Dashboard sent to CFO
**Process Update: 10/2019**
---
In recent quarters, the R compilation has been shifted to Python. Currently, the process calls on the AREC Smartsheet upload (Script to aggregate and upload the acquisitions list into SQL) for compilation.

Using the unique identifier "MEntity", we can link across occupancy and general ledger info.

---
#** General Ledger Income Statement**
---
SAP info uses alternative identifier. First steps involve merging MEntity against SAP profit center number
 
Process Code: "**Income_Statement_Compilation.py**"
Process:
1. Connect to SAP SQL link
2. Wrangle data into a usable format based on pre-determined chart of accounts (Excel Doc)
3. Using the account numbers per line item, aggregate and compile into an income statement
4. Melt Table to create a "Tidy" version; this makes it easy to work with in Excel 

---
#** Occupancy Aggregation**
---



