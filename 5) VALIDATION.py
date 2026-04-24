# Step 5: Validation (NULL check, Duplicate check, Tracking changes using SCD)
# 1) Consistency Check
# 2) Handling Negative values 
# 3) NULL Check 
# 4) Duplicate Check
# 5) Schema Evaluation 
# 6) SCD 1: Order Cancellation 
# 7) SCD 4: Phone Change, Email Change, Adress Change

# Consistency Check
merged_df = merged_df.withColumn(
    "consistency_flag",
    when(col("ord_date") > col("delivery_date"), lit("Inconsistent")).otherwise("Consistent")
)
merged_df.show()

# Handling Negative values 
merged_df = merged_df.withColumn("Delivery_Delay", 
when(datediff(col("delivery_date"), col("estimated_delivery")) < 0, 0) 
.otherwise(datediff(col("delivery_date"), col("estimated_delivery"))) 
)
merged_df.show()

# NULL Check 
merged_df = merged_df \
    .withColumn("cust_id", when(col("cust_id").isNull(), -1).otherwise(col("cust_id"))) \
    .withColumn("ord_id", when(col("ord_id").isNull(), -1).otherwise(col("ord_id"))) \
    .withColumn("cust_name", when(col("cust_name").isNull(), "Unknown Customer").otherwise(col("cust_name"))) \
    .withColumn("email", when(col("email").isNull(), "Unknown Email").otherwise(col("email"))) \
    .withColumn("phone", when(col("phone").isNull(), "Unknown Phone").otherwise(col("phone"))) \
    .withColumn("address", when(col("address").isNull(), "Unknown Address").otherwise(col("address"))) \
    .withColumn("city", when(col("city").isNull(), "Unknown City").otherwise(col("city"))) \
    .withColumn("last_purchase", when(col("last_purchase").isNull(), lit("1900-01-01")).otherwise(col("last_purchase"))) \
    .withColumn("ord_name", when(col("ord_name").isNull(), "Unknown Product").otherwise(col("ord_name"))) \
    .withColumn("price", when(col("price").isNull(), -1).otherwise(col("price"))) \
    .withColumn("ord_date", when(col("ord_date").isNull(), lit("1900-01-01")).otherwise(col("ord_date"))) \
    .withColumn("delivery_date", when(col("delivery_date").isNull(), lit("1900-01-01")).otherwise(col("delivery_date"))) \
    .withColumn("shipping_id", when(col("shipping_id").isNull(), -1).otherwise(col("shipping_id"))) \
    .withColumn("shipping_status", when(col("shipping_status").isNull(), "Unknown Status").otherwise(col("shipping_status"))) \
    .withColumn("estimated_delivery", when(col("estimated_delivery").isNull(), lit("1900-01-01")).otherwise(col("estimated_delivery"))) \
    .withColumn("payment_method", when(col("payment_method").isNull(), "Unknown Payment").otherwise(col("payment_method"))) \
    .withColumn("Order_Processing_Time", when(col("Order_Processing_Time").isNull(), -1).otherwise(col("Order_Processing_Time"))) \
    .withColumn("Delivery_Delay", when(col("Delivery_Delay").isNull(), -1).otherwise(col("Delivery_Delay")))

# Duplicate Check
df_cleaned = merged_df.dropDuplicates()
df_cleaned.show()

# Schema Evaluation 
5)	Schema Evaluation:- 

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType
)

expected_schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("ord_id", IntegerType(), True),
    StructField("cust_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("last_purchase", DateType(), True),
    StructField("ord_name", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("ord_date", DateType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("shipping_id", IntegerType(), True),
    StructField("shipping_status", StringType(), True),
    StructField("estimated_delivery", DateType(), True),
    StructField("payment_method", StringType(), True),
    StructField("Order_Processing_Time", IntegerType(), True),
    StructField("Delivery_Delay", IntegerType(), True)
])
actual_schema = df_cleaned.schema
print(df_cleaned.schema)

if actual_schema == expected_schema:
    print("Schema matches the expected schema")
else:
    print(" Schema mismatch detected!")


# SCD 1: Order Cancellation (Order Cancellation Process):-
cancelled_ord_id = 1100
df_cleaned = df_cleaned.withColumn(
    "shipping_status", 
    when(col("ord_id") == cancelled_ord_id, lit("Cancelled")).otherwise(col("shipping_status"))
)

df_cleaned.show()

# SCD 4: Phone Change, Email Change, Adress Change
CREATE TABLE IF NOT EXISTS merged_data_history (cust_id INT, ord_id INT, cust_name STRING, old_email STRING, old_phone STRING, old_address STRING, changed_column STRING, change_timestamp TIMESTAMP)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

cust_id_to_update = 1001

history_df = df_cleaned.filter(col("cust_id") == cust_id_to_update) \
    .select(col("cust_id"), col("ord_id"), col("cust_name"),
        col("email").alias("old_email"),
        col("phone").alias("old_phone"),
        col("address").alias("old_address"),
        lit("phone/email/address_change").alias("changed_column"),
        current_timestamp().alias("change_timestamp")) 

history_df.write.mode("append").format("hive").saveAsTable("project.merged_data_history")

# Updating new values:
df_cleaned = df_cleaned.withColumn(
    "phone", when(col("cust_id") == 1001, lit("9999999999")).otherwise(col("phone"))
).withColumn(
    "address", when(col("cust_id") == 1001, lit("New Address, Mumbai")).otherwise(col("address"))
).withColumn(
    "email", when(col("cust_id") == 1001, lit("newemail@example.com")).otherwise(col("email"))
)

