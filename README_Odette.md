# Distributed Multi-Model Analytics for E-Commerce Data
## Focuses on Working with MongoDB, HBase and PySpark
###AUCA Big Data Analytics(Final Project:Individual):Odette DUSABEMARIYA 101409


## Project purpose

The e-commerce solution combines customer information, completed purchases and website-session activity. MongoDB holds the operational documents, HBase stores browsing-session records, and Spark processes the exported data to produce analytical datasets.

## 1. Platform responsibilities


| Platform | How it is applied | Data used |
|---|---|---|
| MongoDB | Source database for customer and sales records | `users`, `products`, `transactions` |
| HBase | Persistent store for high-volume session events | `user_sessions` |
| Spark | Cross-source processing and reporting | Extracted MongoDB and HBase data |

Spark uses `user_id` as the common customer reference when bringing the datasets together.


## MongoDB examples

Run MongoDB commands in `mongosh`.

### Select the database and inspect collections
```javascript
use ecommerce_analytics
show collections
```

### Preview customer profiles
```javascript
db.users.find(
  {},
  {
    _id: 0, user_id: 1, city: 1, state: 1, country: 1,
    registration_date: 1, last_active: 1
  }
).limit(5)
```

### Count customers
```javascript
db.users.countDocuments({})
```
The successful Part 3 integration included 10,000 customer profiles.

### Preview transaction records
```javascript
db.transactions.find(
  {},
  {
    _id: 0, transaction_id: 1, session_id: 1, user_id: 1,
    timestamp: 1, subtotal: 1, discount: 1, total: 1,
    payment_method: 1, status: 1
  }
).limit(5)
```

### Group transactions by status
```javascript
db.transactions.aggregate([
  {
    $group: {
      _id: "$status",
      transaction_count: { $sum: 1 },
      transaction_value: { $sum: "$total" }
    }
  },
  { $sort: { transaction_count: -1 } }
])
```

### Calculate completed orders and revenue per customer
```javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  {
    $group: {
      _id: "$user_id",
      completed_orders: { $sum: 1 },
      customer_revenue: { $sum: "$total" },
      average_order_value: { $avg: "$total" }
    }
  },
  { $sort: { customer_revenue: -1 } },
  { $limit: 10 }
])
```

### Compare completed sales by payment method
```javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  {
    $group: {
      _id: "$payment_method",
      transaction_count: { $sum: 1 },
      revenue: { $sum: "$total" }
    }
  },
  { $sort: { revenue: -1 } }
])
```

### Rank products by units sold
```javascript
db.transactions.aggregate([
  { $match: { status: "completed" } },
  { $unwind: "$items" },
  {
    $group: {
      _id: "$items.product_id",
      units_sold: { $sum: "$items.quantity" },
      product_revenue: { $sum: "$items.subtotal" }
    }
  },
  { $sort: { units_sold: -1 } },
  { $limit: 10 }
])
```
`$unwind` expands the transaction's `items` array so that products can be summarized individually.

## HBase examples

Start the shell with `hbase shell`.

### List tables and inspect the session table
```ruby
list
describe 'user_sessions'
```
The table is `user_sessions`. Its five known column families are `activity`, `device`, `location`, `session` and `user`.

### Scan sample rows
```ruby
scan 'user_sessions', { LIMIT => 5 }
```

### Inspect each column family
```ruby
scan 'user_sessions', { COLUMNS => ['activity'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['device'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['location'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['session'], LIMIT => 5 }
scan 'user_sessions', { COLUMNS => ['user'], LIMIT => 5 }
```

### Retrieve one row
The row-key convention is `user_id#start_time#session_id`. Copy a real key from the scan output:
```ruby
get 'user_sessions', 'PASTE_AN_ACTUAL_ROW_KEY_HERE'
```

### Count table rows
```ruby
count 'user_sessions'
```
The export recorded 2,000,000 rows. Spark reported 1,999,999 unique sessions. A row count and a distinct-session count are different measures.

## Apache Spark / PySpark examples

Run these examples from the project directory containing `part3_output`.

### Create a Spark session
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AUCA_Ecommerce_Query_Analysis")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Load the analytical CSV folders
```python
base = "part3_output"

customer_df = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{base}/customer_clv_engagement")
)
segment_df = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{base}/customer_segment_summary")
)
country_df = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{base}/country_performance")
)
engagement_df = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{base}/engagement_vs_purchases")
)
```

### Check the loaded customer data
```python
customer_df.printSchema()
customer_df.show(5, truncate=False)
```

### Show the ten highest-revenue customers
```python
from pyspark.sql.functions import col

(
    customer_df
    .select(
        "user_id", "country", "completed_orders", "lifetime_revenue",
        "average_order_value", "session_count", "total_page_views",
        "customer_segment"
    )
    .orderBy(col("lifetime_revenue").desc())
    .show(10, truncate=False)
)
```

### Summarize customer segments
```python
(
    segment_df
    .select(
        "customer_segment", "customers", "completed_orders", "revenue",
        "average_sessions_per_customer", "average_page_views_per_customer"
    )
    .orderBy(col("revenue").desc())
    .show(truncate=False)
)
```

### Rank countries by revenue
```python
(
    country_df
    .select(
        "country", "customers", "completed_orders", "revenue",
        "average_sessions_per_customer"
    )
    .orderBy(col("revenue").desc())
    .show(10, truncate=False)
)
```

### Compare engagement and purchasing
```python
(
    engagement_df
    .select(
        "engagement_level", "customers", "completed_orders", "revenue",
        "average_customer_revenue", "average_page_views"
    )
    .orderBy(col("average_customer_revenue").desc())
    .show(truncate=False)
)
```

### Calculate revenue/page-view correlation
```python
correlation = customer_df.stat.corr(
    "lifetime_revenue", "total_page_views"
)
print("Revenue/page-view Pearson correlation:", correlation)
```
The reported value was approximately `0.2914`, indicating a modest positive linear association. Correlation does not demonstrate causation.

### Count customers in each segment
```python
(
    customer_df.groupBy("customer_segment")
    .count()
    .orderBy(col("count").desc())
    .show(truncate=False)
)
```

### Run the same type of summary with Spark SQL
```python
customer_df.createOrReplaceTempView("customer_analytics")

query = """
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(completed_orders) AS completed_orders,
    ROUND(SUM(lifetime_revenue), 2) AS total_revenue,
    ROUND(AVG(total_page_views), 2) AS average_page_views
FROM customer_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC
"""

spark.sql(query).show(truncate=False)
```

### Stop Spark
```python
spark.stop()
```

## Integrated workflow

1. MongoDB provides customer profiles and transaction documents from `ecommerce_analytics`. The transaction records represent the extracted snapshot used for this run.
2. HBase provides the `user_sessions` records. The export writes session records to JSON Lines files and supports resuming from a checkpoint.
3. Spark loads the extracted customer, transaction and session data and relates customer information through `user_id`.
4. The processing calculates completed orders, observed-period revenue, average order value, session count, session duration, page views, engagement levels and customer segments.
5. Spark writes an integrated Parquet dataset and analytical CSV folders. The Part 4 program reads the CSV outputs to generate PNG charts.

MongoDB suits the customer and transaction documents, including nested purchase items. HBase stores the large session dataset using row keys and column families. Spark is used for integration, transformations and analytical calculations.

## Recorded Part 3 results

| Measure | Result |
|---|---:|
| Integrated customers | 10,000 |
| Transactions in extracted snapshot | 500,000 |
| Completed transactions | 284,011 |
| HBase rows exported | 2,000,000 |
| Unique HBase sessions reported by Spark | 1,999,999 |
| Customers with completed orders | 10,000 |
| Customers with sessions | 10,000 |
| Revenue/page-view Pearson correlation | 0.2914 |

### Important interpretation notes

- The extracted transaction snapshot contains 500,000 records, not the expected final 500,000 generated records. Use the actual snapshot total in the report.
- The HBase export row count and Spark's distinct-session count are not interchangeable. The reported values are 2,000,000 rows and 1,999,999 unique sessions.
- `lifetime_revenue` is an observed-period revenue measure and a CLV proxy. It is not a predictive lifetime-value model because future purchases are not forecast.
- The correlation of 0.2914 is positive but modest; it does not prove that browsing activity causes purchases.

## Part 4 visualization

Run the visualization script from the project root:

```bash
python part4_visualizations.py --input part3_output --output part4_output
```

Expected files:

```text
part4_output/
    01_customer_revenue_distribution.png
    02_customer_segment_comparison.png
    03_top_countries_by_revenue.png
    04_engagement_vs_average_revenue.png
    part4_interpretation.txt
```

Include the four PNG charts in the final report and explain the findings visible in each chart.

## Running reminders

- Use `mongosh` for MongoDB statements and `hbase shell` for HBase commands.
- Run PySpark in the Python environment configured for this project.
- Start MongoDB and HBase services before connecting to their shells.
- Run Spark examples from the project root so that `part3_output` resolves, or replace it with the full path.
- Replace the HBase row-key placeholder with an actual key from `scan`.
- Inspect the HBase scan output before referring to individual column qualifiers.
- Stop Spark with `spark.stop()` after analysis.
