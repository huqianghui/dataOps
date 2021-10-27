# Databricks notebook source
# MAGIC %python
# MAGIC # Azure Storage Account Name
# MAGIC storage_account_name = "mlaztest2535368501"
# MAGIC 
# MAGIC # Azure Storage Account Key
# MAGIC storage_account_key = "2NzN84l5kqVO33nZIrUd67AWuTw3CIHzdFZSKBPzIOKXXzGHgMDdWGj2QIIQ0ZOlSxh7ao+7pYrHjQZftEgCZg=="
# MAGIC 
# MAGIC # Azure Storage Account Source Container
# MAGIC container = "split-cap"
# MAGIC 
# MAGIC # Set the configuration details to read/write
# MAGIC spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/rosefinch@qianghuihu.partner.onmschina.cn")

# COMMAND ----------


# Check if file exists in mounted filesystem, if not create the file
if "split-cap/" not in [file.name for file in dbutils.fs.ls("/mnt/FileStore/shared_uploads")]:
  dbutils.fs.mount(
  source = "wasbs://{0}@{1}.blob.core.chinacloudapi.cn".format(container, storage_account_name),
  mount_point = "/mnt/FileStore/shared_uploads/split-cap",
  extra_configs = {"fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(storage_account_name): storage_account_key}
 )
  

# COMMAND ----------

# MAGIC %python
# MAGIC # Check is all files exist
# MAGIC 
# MAGIC dbutils.fs.ls("dbfs:/mnt/FileStore/shared_uploads/split-cap")

# COMMAND ----------

pip install scapy

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/mnt/FileStore/shared_uploads/split-cap/

# COMMAND ----------

# from scapy.all import *

# ftxt = open('/dbfs/mnt/FileStore/shared_uploads/cap/testCap3.txt', 'w')
# ftxt.write(str(rdpcap("/dbfs/mnt/FileStore/shared_uploads/cap/test.cap")))

# COMMAND ----------

# with open("/dbfs/mnt/FileStore/shared_uploads/cap/testCap3.txt", "r") as f_read:
#  for line in f_read:
#    print(line)

# COMMAND ----------

# import sys
# import math
# from collections import defaultdict
# import numpy as np

# a = rdpcap("/dbfs/mnt/FileStore/shared_uploads/cap/test.cap")

# Stats = np.zeros((14))

# print(len(a))

# for p in a:
#        pkt = p.payload
#        #Management packets
#        if p.haslayer(Dot11) and p.type == 0:
#                ipcounter = ipcounter +1
#                Stats[p.subtype] = Stats[p.subtype] + 1
# print(Stats)

# COMMAND ----------

from scapy.all import *
import sys
import math
from collections import defaultdict
import numpy as np

allPackets = rdpcap("/dbfs/mnt/FileStore/shared_uploads/split-cap/test-split_00003_20211021180831.cap")

Stats = np.zeros((14))
count = 3
index =0

for session in allPackets.sessions():
  index = index +1
  if(index < count):
    print(session)

# COMMAND ----------

print(allPackets[0].mysummary())
print(allPackets[0].mysummary)

# COMMAND ----------

raw(allPackets[0])

# COMMAND ----------

hexdump(allPackets[0])

# COMMAND ----------

ls(allPackets[0])

# COMMAND ----------

allPackets[0].show()

# COMMAND ----------

for field in allPackets[0].getlayer(1).fields:
  srcIp = getattr(allPackets[0]["IP"], "src")
  dstIp = getattr(allPackets[0]["IP"], "dst")
  print(": " + field_value)

# COMMAND ----------

# import pyspark class Row from module sql
from pyspark.sql import *

CapResult = Row("Time", "Source", "Destination","Sport","Dport" ,"Protocol","Length","Info")
cap1=CapResult("2021-10-21 17:07:19.810486","172.16.6.187","172.16.12.166","40774","6380","TCP","66","40774 → 6380 [ACK] Seq=1 Ack=2 Win=283 Len=0 SLE=1 SRE=2")

capResultSeq1=[cap1]
capDataframe= spark.createDataFrame(capResultSeq1)

capDataframe.show()

capDataframe.write.format("delta").save("/mnt/delta/cap/") 

# COMMAND ----------

allPackets[0].sprintf('TCP {IP:%IP.src%}{IPv6:%IPv6.src%}:%r,TCP.sport% > {IP:%IP.dst%}{IPv6:%IPv6.dst%}:%r,TCP.dport%')

# COMMAND ----------

allPackets[0].decode_payload_as()

# COMMAND ----------

allPackets[0].command()

# COMMAND ----------

for field in allPackets[0].fields:
   print(field)
    
allPackets[0].getlayer(3)