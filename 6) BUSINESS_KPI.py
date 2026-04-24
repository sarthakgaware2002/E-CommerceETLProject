# Step 6: Business KPIs and Performance Metrics

# 1) Average Order Value (AOV)
# 2) Repeat Customer Count
# 3) Order Cancellation Rate
# 4) Total Orders Processed Daily
# 5) Order Success Rate (Only Delivered Orders)
# 6) Top Customers by Revenue

# 1) Average Order Value (AOV)
total_revenue = df_cleaned.agg(sum("price").alias("total_revenue")).collect()[0]["total_revenue"]

total_orders = df_cleaned.agg(countDistinct("ord_id").alias("total_orders")).collect()[0]["total_orders"]  

aov_df = spark.createDataFrame([(round(aov, 2),)], ["Average_Order_Value"])
aov_df.show()


# 2) Repeat Customer Count
customer_order_counts = df_cleaned.groupBy("cust_id").agg(count("ord_id").alias("order_count"))

        repeat_customers_df = customer_order_counts.filter(col("order_count") > 1) .count()

       repeat_customers = repeat_customers_df.select("cust_id").distinct().count() 

total_unique_customers = df_cleaned.select("cust_id").distinct().count()

repeat_customer_rate = (repeat_customers / total_unique_customers * 100) if total_unique_customers != 0 else 0


# 3) Order Cancellation Rate
total_orders = df_cleaned.select("ord_id").distinct().count()

cancelled_orders_df = df_cleaned.filter(col("shipping_status") == "Cancelled")

cancelled_orders = cancelled_orders_df.select("ord_id").distinct().count()

cancellation_rate = (cancelled_orders / total_orders * 100) if total_orders != 0 else 0


# 4) Total Orders Processed Daily
total_orders = df_cleaned.select("ord_id").distinct().count()

min_date = df_cleaned.agg(min("ord_date")).collect()[0][0]

max_date = df_cleaned.agg(max("ord_date")).collect()[0][0]

number_of_days = (max_date - min_date).days + 1

average_daily_orders = round(total_orders / number_of_days, 2)

daily_order_trend = df_cleaned.groupBy("ord_date").agg(count("ord_id").alias("daily_order_count")).orderBy("ord_date")


# 5) Order Success Rate (Only Delivered Orders)
total_orders = df_cleaned.select("ord_id").distinct().count()

delivered_orders = df_cleaned.filter(col("shipping_status") == "Delivered") \
                             .select("ord_id").distinct().count()

order_success_rate = (delivered_orders / total_orders) * 100


# 6) Top Customers by Revenue
top_customers_df = df_cleaned.groupBy("cust_id").agg(sum("price").alias("total_spent")).orderBy(col("total_spent").desc())

top_customers_df.show(10)
