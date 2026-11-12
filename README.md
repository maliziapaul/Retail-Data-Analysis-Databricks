# Retail Data Analysis (Databricks)

PySpark analysis of the `retail-data/by-day` dataset (305 daily CSV files of online retail sales), written as a Databricks notebook.

**Product Value = Unit Price × Quantity**

## Results

| Part | Result |
|---|---|
| b) Total transactions | **541,909** rows (25,900 distinct invoices) |
| b) Total product value | **9,747,747.93** (net of returns and cancellations) |
| c) Top 5 by quantity | World War 2 Gliders Asstd Designs 53,847 · Jumbo Bag Red Retrospot 47,363 · Assorted Colour Bird Ornament 36,381 · Popcorn Holder 36,334 · Pack of 72 Retrospot Cake Cases 36,039 |
| d) Top 5 by sales value | DOTCOM POSTAGE 206,245.48 · Regency Cakestand 3 Tier 164,762.19 · White Hanging Heart T-Light Holder 99,668.47 · Party Bunting 98,302.98 · Jumbo Bag Red Retrospot 92,356.03 |
| e) By country | 38 countries, led by the United Kingdom (8,187,806.36), the Netherlands (284,661.54) and EIRE (263,276.82) |

DOTCOM POSTAGE is a shipping charge recorded as a line item. It is kept because the question covers all transactions.

## Files

- `Retail_Data_Analysis.py`: the notebook in Databricks source format. Import it into Databricks (**Workspace → Import → File**).
- `test_local.py`: runs the same notebook on a local Spark install, with stand-ins for Databricks' `display()` and `dbutils`, to check the numbers.

The dataset is not included in this repository.

## Running it in Databricks

1. **Upload the data:** **Catalog → workspace → default → Create → Volume**, name it `retail_data`, then **Upload to this volume** and select all 305 CSV files.
2. **Import the notebook:** **Workspace → Import → File** and choose `Retail_Data_Analysis.py`.
3. **Run it:** **Run all**, using serverless compute.
4. **Chart for part f:** on the last result table, click **+ → Visualization**, choose **Bar**, set X to `Description` and Y to `TotalSalesValue`, then **Save**.
5. **Export:** **File → Export → Source file**.
