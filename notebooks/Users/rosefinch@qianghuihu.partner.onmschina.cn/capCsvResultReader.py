# Databricks notebook source
storage_account_name = "mlaztest2535368501"
storage_account_key = "2NzN84l5kqVO33nZIrUd67AWuTw3CIHzdFZSKBPzIOKXXzGHgMDdWGj2QIIQ0ZOlSxh7ao+7pYrHjQZftEgCZg=="
container = "csv"
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

if "csv/" not in [file.name for file in dbutils.fs.ls("/mnt")]:
    dbutils.fs.mount(
      source = "wasbs://{0}@{1}.blob.core.chinacloudapi.cn".format(container, storage_account_name),
      mount_point = "/mnt/csv/",
      extra_configs = {"fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(storage_account_name): storage_account_key}
     )

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *

schema = StructType([StructField("No.",LongType(),False),
                    StructField("Time",DoubleType(),False),
                    StructField("Destination",StringType(),False),
                    StructField("Source",StringType(),True),
                    StructField("Protocol",StringType(),False),
                    StructField("Length",IntegerType(),False),
                    StructField("Text item",StringType(),False),
                    StructField("Info",StringType(),False)
                    ])

# COMMAND ----------

# File location and type
file_location = "/mnt/csv/"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.readStream.schema(schema).format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .csv(file_location)

display(df)

# COMMAND ----------

df1 = df.drop("No.")

# COMMAND ----------

df2= df1.drop("Text item")

# COMMAND ----------

df3=df2.withColumnRenamed("Relative Time","Time")

# COMMAND ----------

df3.writeStream.format("delta").outputMode("append").option("checkpointLocation","/mnt/datalake1demo1111/deltatables/packetRecords/checkpoint").start("/mnt/datalake1demo1111/deltatables/packetRecords")


# COMMAND ----------

# Create the table

spark.sql("CREATE TABLE " + "default.packetRecords" + " USING DELTA LOCATION '" + "/mnt/datalake1demo1111/deltatables/packetRecords" + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from default.packetRecords

# COMMAND ----------

