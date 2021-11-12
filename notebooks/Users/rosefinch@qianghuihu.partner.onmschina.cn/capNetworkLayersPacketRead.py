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
                    StructField("ack",LongType(),False),
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

# capDataframe.write.format("delta").save("/mnt/delta/cap/") 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "681d9f52-5c95-4f5f-921d-b75ac0128acc",
          "fs.azure.account.oauth2.client.secret": "NwV_q37.tM.c-ghtWavL1W9-Hp43cN3Jmf",
          "fs.azure.account.oauth2.client.endpoint": "https://login.partner.microsoftonline.cn/2f72f96c-65f9-4a6a-b166-dd61493e4b2e/oauth2/token"}

if "datalake1demo1111/"  in [file.name for file in dbutils.fs.ls("/mnt")]:
    dbutils.fs.unmount("/mnt/datalake1demo1111")
    
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://tables@datalake1demo.dfs.core.chinacloudapi.cn/",
  mount_point = "/mnt/datalake1demo1111",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/datalake1demo1111")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/datalake1demo1111/deltatables

# COMMAND ----------

capDataframe.write.format("delta").mode("overwrite").saveAsTable("default.packets")

# COMMAND ----------

type(capDataframe)

# COMMAND ----------

from scapy.all import *
import sys
import math
from collections import defaultdict
import numpy as np

layers=3

# COMMAND ----------

import pandas as pd
# import pyspark class Row from module sql
from pyspark.sql import *
from pyspark.sql.types import *

allPackets = rdpcap("/dbfs/FileStore/sample/first5_test.cap")
lenght=len(allPackets)

lenght=1
elementNum=0;
spark.conf.set("spark.sql.execution.arrow.enabled", "false")


for index in range(lenght):
    elementValues = []
    for layerNo in range(layers):
        for field in allPackets[0].getlayer(layerNo).fields:
            fieldValue = getattr(allPackets[index][layerNo], field)
            if(type(fieldValue) == scapy.fields.FlagValue):
                elementValues.append(str(fieldValue))
            else :
                elementValues.append(fieldValue)
    df = pd.DataFrame(elementValues).T.apply(lambda x:x.apply(lambda x: [""] if x == [] else x))
    sqlDf=spark.createDataFrame(df, schema)
    sqlDf.write.mode("append").save("/mnt/datalake1demo1111/deltatables/packets");
    

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/datalake1demo1111/deltatables/packets")
df.show()

# COMMAND ----------

# Create the table.
spark.sql("DROP TABLE default.packets")

spark.sql("CREATE TABLE " + "default.packets" + " USING DELTA LOCATION '" + "/mnt/datalake1demo1111/deltatables/packets" + "'")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from default.packets