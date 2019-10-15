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

Before the shift to Python, the following steps were all compiled within a single R Script "**Quarterly Acquisitions Report.RMD**"
1. Income Statement Compilation
2. Occupancy Metrics
3. Forecasted Occupancy Metrics

---
General Ledger Income Statement
---
SAP info uses alternative identifier. First steps involve merging MEntity against SAP profit center number
 
* Process Code: "**Income_Statement_Compilation.py**"
* Language: Python
Process:
1. Connect to SAP SQL link
2. Wrangle data into a usable format based on pre-determined chart of accounts (Excel Doc)
3. Using the account numbers per line item, aggregate and compile into an income statement
4. Melt Table to create a "Tidy" version; this makes it easy to work with in Excel 

---
Occupancy Aggregation
---
* Process Code: "**Quarterly Acquisitions Report.RMD**"
* Language: R
Process:
1. Use MEntity number to filter WSS DB (SQL Table)
2. Iterate process for relevant MEntity numbers
3. Import Forecasted Occupancy metrics from a separate SQL Table 

---
Dashboard Compilation
---
Once the steps above are complete, export into csv/xlsx. From there, a dashboard template reads in the export using Excel's Power Pivot feature. The goal is to move away from Excel where possible. As such, Tableau and SQL will be used to replace the current reporting process. Executive buy in may not succeed, as such, both versions will be presented in parallel. 


