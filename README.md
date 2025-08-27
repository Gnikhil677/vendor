# Project: Advanced Data Analysis on Vendor Sales & Purchases

## **Overview:**

Made an end-to-end Company Standard Project where I worked on a large-scale real-world dataset (>2GB, several million rows) involving vendor sales and purchase transactions. This was the largest dataset I’ve analyzed to date and required building an efficient end-to-end pipeline integrating **PostgreSQL, Python, and Power BI**.

**Key Contributions:**

* **Data Pipeline Development:** Designed and implemented a pipeline to import raw vendor sales & purchase data directly from PostgreSQL into Python.
* **SQL-based Preprocessing:** Since the dataset was too large to handle directly, optimized queries to fetch only the required subset of data. This reduced memory usage and improved performance significantly.
* **Data Cleaning & Transformation:** Using Python (Pandas + custom scripts), applied advanced cleaning steps, created new calculated columns, and standardized formats for analysis.
* **Database Optimization:** Implemented optimization techniques (indexes, efficient joins, query tuning) to improve data retrieval speed.
* **Automated Data Flow:** Built a single Python script to (1) fetch raw data → (2) clean & transform → (3) push back to PostgreSQL as a new table `"vendor_cleaned"`.
* **Visualization & Reporting:** Connected Python to the database and performed EDA, answered industry based questions and built advanced visualizations using matplotlib and seaborn.

    Connected Power BI to the cleaned table, built interactive dashboards analyzing vendor purchase trends, sales performance, cost efficiencies, and anomalies.

### **Impact:**

* Reduced dataset size and improved loading times by **over 90%**, making downstream analysis and visualization feasible.
* Created a sustainable pipeline that can be re-run for future data updates without manual intervention.
* Delivered actionable insights on vendor performance and purchasing efficiency through Python & Power BI dashboards.

### Tools Used

* DBeaver
* pgAdmin4
* PostgreSQL
* Python (Pandas, NumPy, Matplotlib, Seaborn)
* Power BI
* DAX
* ChatGPT
* Visual Studio Code
* GitHub


**Dataset:**
This project was inspired by a dataset and guidance from *Tech Classes* on YouTube. Following along with the tutorial helped me get started and understand the basics of handling large CSV files and loading them into a database. From there, I expanded the project further and built my own advanced pipeline and dashboards, learning several new techniques along the way.


**PostgreSQL Data Import:**
As the first step, I created a **new database in pgAdmin4** to store the raw dataset.
Once the database was set up, I imported the CSV file (over **2GB, millions of rows**) into tables using the following SQL command:

```sql
COPY purchases(column1, column2, column3, ...)
FROM 'C:/path/to/your/file.csv'
DELIMITER ','
CSV HEADER;
```

This allowed me to efficiently bring the dataset into PostgreSQL, forming the foundation for further cleaning, processing, and analysis.


## SQL EDA & Data Preparation

After successfully importing the data into PostgreSQL, I performed **exploratory data analysis (EDA) using SQL**.

* Explored different tables and columns to identify what was necessary for advanced analysis.
* Used **CTEs (Common Table Expressions)** to merge multiple tables efficiently into a single, optimized dataset.

  * Initially, direct joins on such a large dataset were failing due to memory and performance issues.
  * CTEs allowed me to break down the process step by step, making the merges more manageable and optimized.


## Python Cleaning & Transformation

Once I had the optimized merged table from SQL, I moved into Python for additional cleaning and transformations:

* Connected Python to PostgreSQL using libraries like **psycopg2/SQLAlchemy**.
* Added new calculated columns to support further analysis (e.g., cost efficiency, vendor-level aggregations).
* Performed **basic data cleaning** (handling missing values, formatting, standardization).
* Automated the process with a **single Python script** that:

  1. Pulled data from PostgreSQL.
  2. Cleaned & enriched the data.
  3. Pushed the processed data back to PostgreSQL into a new table `"vendor_cleaned"`.

The script (shown below) demonstrates the entire workflow:

```python
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/vendor")

query = """
WITH Freight_summary AS (
	SELECT "VendorNumber", SUM("Freight") AS FreightCost 
	FROM vendor_invoice vi 
	GROUP BY "VendorNumber" 
	),
Purchase_summary AS (
	SELECT 
		p."VendorNumber",
		p."VendorName",
		p."Brand",
		p."Description",
		p."PurchasePrice",
		pp."Volume",
		pp."Price" AS ActualPrice,
		SUM(p."Quantity") AS TotalPurchaseQuantity,
		SUM(p."Dollars") AS TotalPurchaseDollars
	FROM purchases p
	JOIN purchase_prices pp 
		ON p."Brand" = pp."Brand"
	WHERE p."PurchasePrice" > 0
	GROUP BY p."VendorNumber", p."VendorName", p."Brand", p."Description", p."PurchasePrice", pp."Volume", pp."Price"
	 ) ,
sales_summary AS
	(SELECT 
		"VendorNo",
		"Brand",
		SUM("SalesQuantity") as TotalSalesQuantity,
		SUM("SalesPrice") AS TotalSalesPrice,
		SUM("SalesDollars") AS TotalSalesDollars,
		SUM("ExciseTax") AS TotalExciseTax
	FROM sales s 
	GROUP BY "VendorNo", "Brand" 
	),
vendor_table as	(
SELECT 
	ps."VendorNumber",
	ps."VendorName",
	ps."Brand",
	ps."Description",
	ps."PurchasePrice",
	ps.actualprice,
	ps."Volume",
	ps.totalpurchasedollars,
	ps.totalpurchasequantity,
	cs.freightcost,
	ss.totalsalesprice,
	ss.totalsalesquantity,
	ss.totalsalesdollars,
	ss.totalexcisetax
FROM Purchase_summary ps
LEFT JOIN sales_summary ss
	ON ps."VendorNumber" = ss."VendorNo"
	AND ps."Brand" = ss."Brand"
LEFT JOIN Freight_summary cs
	ON ps."VendorNumber" = cs."VendorNumber" 
ORDER BY ps.totalpurchasedollars DESC 
)
select * from vendor_table;
"""
df = pd.read_sql(query, engine)

#CLEANING AND ADDING COLUMNS

#assigning correct data type
df['Volume'] = df['Volume'].astype('float64')

#cleaning
df.fillna(0, inplace=True)

df['VendorName'] = df['VendorName'].str.strip()


#adding columns
df['GrossProfit'] = df['totalsalesdollars'] - df['totalpurchasedollars']
df['ProfitMargin'] = (df['GrossProfit']/df['totalsalesdollars'])*100
df['StockTurnover'] = df['totalsalesquantity']/df['totalpurchasequantity']
df['SalesToPurchaseRatio'] = df['totalsalesdollars']/df['totalpurchasedollars']

df.to_sql("vendor_cleaned", engine, if_exists="replace", index=False)

print("✅ Cleaned data uploaded to PostgreSQL table: vendor_cleaned")
```
The script is also available in the files.


## Python EDA & Business Insights

After the initial SQL-based cleaning and optimization, I used **Python (Pandas, Matplotlib, Seaborn)** to perform detailed exploratory data analysis (EDA). This step allowed me to understand the **data distribution, correlations, and patterns**, while also answering key **industry-based business questions**.

Visit My Notebook for Detailed Steps & to see the code - [notebook](notebook.ipynb)


### Data Distribution & Outlier Analysis

![distribution](images/distribution.png)
![boxplot](images/boxplot_dist.png)

1. **Gross Profit & Profit Margin**
   - Both variables show a significant number of **negative values**, indicating cases where **costs exceeded revenue**.  
   - This reflects potential losses or pricing inefficiencies in certain transactions.

2. **Highly Skewed Distributions**
   - Most numerical variables (e.g., `PurchasePrice`, `Volume`, `totalsalesdollars`, `freightcost`) are **right-skewed**, with a large concentration near zero and a few extreme high values.  
   - This suggests the dataset has **long-tail behavior**, where a few records dominate the scale.

3. **Presence of Outliers**
   - Boxplots reveal strong **outliers across almost all variables** (e.g., `freightcost`, `totalsalesdollars`, `totalpurchasedollars`).  
   - These outliers can heavily influence mean-based metrics and may require **transformation or capping** before modeling.

4. **Sales vs Purchases**
   - `SalesToPurchaseRatio` shows extreme variation, with some products having ratios far beyond the typical range, suggesting either **data entry errors** or **exceptional high-performing items**.

5. **Stock Turnover**
   - Majority of items have very low turnover, while a few show extremely high turnover, indicating **imbalanced inventory movement**.


**Correlation Heatmap**

![Corr_heatmap](images/correlation_heatmap.png)

### Insights:

1. **Sales and Purchase Values are Highly Correlated**

   * Strong positive correlation between `totalsalesdollars`, `totalpurchasedollars`, and `totalexcisetax`.
   * **This indicates that higher purchases and taxes consistently translate into higher sales values.**

2. **Gross Profit Declines with Higher Sales & Quantity**

   * `GrossProfit` has strong negative correlation with `totalsalesdollars` (-0.88) and `totalsalesquantity` (-0.97).
   * **This indicates that selling more does not always increase profitability, possibly due to higher costs or lower margins.**

3. **Profit Margin is Independent of Volume**

   * Very weak correlation of `ProfitMargin` with sales, purchases, or quantity.
   * **This indicates that margin is driven by pricing and operational strategies, not just sales volume.**

## **Key Questions Addressed:**

1. **Which Vendors and Brands contribute the highest sales performance?**

   * Identified top-10 performing vendors and brands driving overall sales.

![Top10Vendors](images/Top_10_vendors&brands.png)

2. **Which Vendors contribute the most to total purchase dollars?**

   * Ranked vendors by their dollar contribution percentage & cumulative percentages to total purchases.

![pur_contri](images/Top_vendors_purchase_contri.png)

3. **What is the total contribution by the Top 10 Vendors?**

   * Created a pie chart showing the purchase contribution of the Top 10 Vendors vs. Others.

   * Results: Top 10 Vendors = 65% of total purchases, while Others = 35%.

![pie_contri](images/Purchase_Contribution_pie.png)

4. **Does purchasing in Bulk Quantities reduce the unit price?**

   * Analyzed unit price distribution across purchase volumes.
   * Found that higher purchase quantities generally lead to more stable unit prices and cost savings.

![bulk_boxplot](images/Bulk_pur_boxplot.png)

As we can see, **buying in small quantities results in higher average unit prices** and also introduces **large outliers**.  
In contrast, **medium and large quantities show more stable unit prices** with almost no outliers.  

✅ This indicates that **purchasing in larger or medium quantities is beneficial**, as it allows vendors to **reduce costs and achieve price stability**.


5. **Which Vendors have low inventory turnover (slow-moving products)?**

   * Detected vendors with inventory inefficiencies with stock turnover < 1, signaling potential overstock issues.

![lowST](images/low_stockturnover.png)

6. **How much unsold inventory exists per vendor, and which vendor contributes the most?**

   * Measured unsold stock across vendors and identified the Top 10 contributors.

![unsold_inv](images/vendor_with_unsold_inventory.png)


**Insights & Visualizations:**

* Created detailed plots and statistical summaries showing distribution, vendor comparisons, and cost efficiencies.
* Answered business questions with **data-backed evidence** and **clear visual storytelling**.
* Delivered **in-depth insights in a Jupyter Notebook**, combining both code and visualizations for transparency and reproducibility.

Perfect 👍 this should come in as your **final section** of the report, highlighting how you took the cleaned data all the way into **Power BI** and applied **advanced DAX** for business-ready dashboards. Here’s how you can write it:

---

### Power BI Dashboard & Advanced Visualization

![Dashboard](images/Vendor_dashboard.png)

As the final step, I connected **Power BI** directly to the cleaned `"vendor_cleaned"` table stored in PostgreSQL.

* Imported the optimized dataset into Power BI for interactive reporting.
* Designed dashboards with a **focus on vendor sales, purchases, inventory efficiency, and cost optimization**.
* Added supporting reference tables for drill-down analysis and better relational modeling.
* Applied **advanced DAX measures.**

**Outcome:**

* Built an **interactive, business-ready dashboard** that allows stakeholders to explore vendor performance and purchasing patterns in real time.
* Enabled **data-driven decision-making**, such as identifying cost-saving opportunities in bulk purchases and tracking slow-moving inventory.


## Conclusion

This project stands as the **largest and most advanced data analysis project I have worked on**, dealing with a dataset of over **2GB and several million rows**. It provided me the opportunity to combine multiple skills and tools—**PostgreSQL, Python, and Power BI**—into a single end-to-end workflow.

Starting from raw data import and SQL-based optimization, to Python-driven cleaning, exploratory analysis, and finally building an interactive Power BI dashboard with advanced DAX, I was able to create a **robust pipeline** that delivered actionable business insights.

This experience not only improved my technical expertise in **handling big data, query optimization and visualization**, but also taught me how to approach real-world industry problems, answer business-driven questions, and present insights effectively.

Overall, the project reflects my ability to integrate **database management, programming, and business intelligence tools** to transform raw, large-scale data into meaningful, decision-ready insights.
