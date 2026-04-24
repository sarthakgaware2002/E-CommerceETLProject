# Step 8: Load Data from Hive into S3 L2


s3_output_path = " s3://s3l2/finaldata/"

hive_df.coalesce(1).write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(s3_output_path)
