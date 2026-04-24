# Step 3: Read JSON data in Mongodb using Pandas in S3 L1
import pandas as pd
from pymongo import MongoClient
import boto3
from io import StringIO

# ------------------------
# 1️⃣ Read data from MongoDB
# ------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["local"]
collection = db["ecomjson"]

data = list(collection.find({}))
df = pd.DataFrame(data).drop(columns=["_id"], errors="ignore")

# ------------------------
# 2️⃣ Convert DataFrame to CSV in memory
# ------------------------
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# ------------------------
# 3️⃣ Upload CSV to S3
# ------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id="xxxxxxxxxxxxxxxxxxxx",        # Or use IAM role if running on EC2
    aws_secret_access_key="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    region_name="ap-south-1"                        # Change to your AWS region
)

bucket_name = "mongo2002"
file_key = "jsondata/mongodata.csv"  # Path inside S3

s3.put_object(
    Bucket=bucket_name,
    Key=file_key,
    Body=csv_buffer.getvalue()
)

print(f"✅ Data uploaded successfully to s3://{bucket_name}/{file_key}")
