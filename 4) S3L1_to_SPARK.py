# Step 4: Reading data from S3 L1 into spark:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

mongo_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://mongo2002/jsondata/mongo_data.csv")

# Merge both records RDS + Mongo DB
merged_df = batch_df.union(mongo_df)
merged_df.show()

# Add two new derived columns for enhanced analysis:
# Order_Processing_Time = delivery_date – ord_date
# Delivery Delay = delivery_date - estimated_delivery  

merged_df = merged_df.withColumn("Order_Processing_Time", datediff(col("delivery_date"), col("ord_date"))) \
          .withColumn("Delivery_Delay", datediff(col("delivery_date"), col("estimated_delivery"))) 









