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

lenght = 5

for index in range(lenght):
    for layerNo in range(layers):
        for field in allPackets[0].getlayer(layerNo).fields:
            fieldValue = getattr(allPackets[index][layerNo], field)
            print("field: " + str(field) + " value: " + str(fieldValue))


# COMMAND ----------

print(len(allPackets))
# fieldName="src"
# fieldValue = getattr(allPackets[0][1], fieldName)
# print("fieldValue: " + str(fieldValue))

# COMMAND ----------

