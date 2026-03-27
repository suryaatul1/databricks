
import dlt

def fr_daily_customer_books():
  return spark.sql(""" 
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
  """)


@dlt.table(
  name="france_daily_customer_books",
  comment="Daily books count per customer in France"
)
def daily_customer_books():
  return fr_daily_customer_books()