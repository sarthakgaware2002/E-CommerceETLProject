# Step 2: Read data from RDS into Spark for Transformation 
# Adding JDBC Jar into EMR:
whereis spark
spark: /usr/lib/spark/jars
wget https://jdbc.postgresql.org/download/postgresql-42.2.14.jar
sudo mv /home/hadoop/ postgresql-42.2.14.jar /usr/lib/spark/jars

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Read data from RDS into Spark
spark = SparkSession.builder \
    .appName("ReadDataFromPostgreSQL") \
    .getOrCreate()

cust_df=spark.read.format("jdbc").option("url","jdbc:postgresql://database-1.czuow2mqoxsb.ap-south-1.rds.amazonaws.com:5432/ecom").option("user","sarthak").option("password","sarthakgaware").option("driver","org.postgresql.Driver").option("dbtable","cust").load()

ord_df=spark.read.format("jdbc").option("url","jdbc:postgresql://database-1.czuow2mqoxsb.ap-south-1.rds.amazonaws.com:5432/ecom").option("user","sarthak").option("password","sarthakgaware").option("driver","org.postgresql.Driver").option("dbtable","ord").load() 
shipping_df=spark.read.format("jdbc").option("url","jdbc:postgresql://database-1.czuow2mqoxsb.ap-south-1.rds.amazonaws.com:5432/ecom").option("user","sarthak").option("password","sarthakgaware").option("driver","org.postgresql.Driver").option("dbtable","shipping").load()

cust_df.show()
ord_df.show()
shipping_df.show()

# JOIN
batch_df = cust_df.join(ord_df, "cust_id", "inner") \
 .join(shipping_df, ["cust_id", "ord_id"], "inner")

batch_df.show()

