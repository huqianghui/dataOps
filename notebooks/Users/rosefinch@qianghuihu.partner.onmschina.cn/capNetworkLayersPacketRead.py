# Databricks notebook source
storage_account_name = "mlaztest2535368501"
storage_account_key = "2NzN84l5kqVO33nZIrUd67AWuTw3CIHzdFZSKBPzIOKXXzGHgMDdWGj2QIIQ0ZOlSxh7ao+7pYrHjQZftEgCZg=="
container = "split-cap"
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

if "split-cap/" not in [file.name for file in dbutils.fs.ls("/mnt")]:
    dbutils.fs.mount(
      source = "wasbs://{0}@{1}.blob.core.chinacloudapi.cn".format(container, storage_account_name),
      mount_point = "/mnt/{0}".format(container),
      extra_configs = {"fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(storage_account_name): storage_account_key}
     )

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/split-cap

# COMMAND ----------

from scapy.all import *
import sys
import math
from collections import defaultdict
import numpy as np

allPackets = rdpcap("/dbfs/mnt/split-cap/test-split_00000_20211021170719.cap")

# COMMAND ----------

layers=3
lenght=len(allPackets)

# COMMAND ----------

lenght = 1

for index in range(lenght):
    for layerNo in range(layers):
        for field in allPackets[0].getlayer(layerNo).fields:
            fieldValue = getattr(allPackets[index][layerNo], field)
            print("field: " + str(field) + " value: " + str(fieldValue))


# COMMAND ----------

# import pyspark class Row from module sql
from pyspark.sql import *
from pyspark.sql.types import *

schema = StructType([StructField("mac_dst",StringType(),False),
                    StructField("mac_src",StringType(),False),
                    StructField("type",IntegerType(),False),
                    StructField("ip_options",ArrayType(StringType(),True),True),
                    StructField("version",IntegerType(),False),
                    StructField("ihl",StringType(),True),
                    StructField("tos",IntegerType(),False),
                    StructField("len",IntegerType(),False),
                    StructField("id",IntegerType(),False),
                    StructField("ip_flags",StringType(),False),
                    StructField("frag",IntegerType(),False),
                    StructField("ttl",IntegerType(),False),
                    StructField("proto",IntegerType(),False),
                    StructField("ip_chksum",IntegerType(),True),
                    StructField("ip_src",StringType(),False),
                    StructField("ip_dst",StringType(),False),
                    StructField("sport",IntegerType(),False),
                    StructField("dport",IntegerType(),False),
                    StructField("seq",IntegerType(),False), 
                    StructField("ack",IntegerType(),False),
                    StructField("dataofs",IntegerType(),False),
                    StructField("tcp_reserved",IntegerType(),False),
                    StructField("tcp_flags",StringType(),False),
                    StructField("window",IntegerType(),False),
                    StructField("tcp_chksum",IntegerType(),False),
                    StructField("urgptr",IntegerType(),False),
                    StructField("tcp_options",ArrayType(StringType(),True),True)   
                    ])

# define emtpy dataframe
capDataframe= spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

capDataframe.show()

capDataframe.write.format("delta").save("/mnt/delta/cap/") 

# COMMAND ----------

