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