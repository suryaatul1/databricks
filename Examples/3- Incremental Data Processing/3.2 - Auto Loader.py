# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC use schema labuser14322013_1774521349;

# COMMAND ----------

#%run ../Includes/Copy-Datasets
volume= spark.sql("describe volume bookstore").take(1)
print(volume)

# COMMAND ----------


dataset_bookstore=f"/Volumes/{volume[0]['catalog']}/{volume[0]['database']}/{volume[0]['name']}"
print(dataset_bookstore)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")

display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create volume if not exists autoloader_chkpoint;
# MAGIC     
# MAGIC

# COMMAND ----------

chkpt= spark.sql("describe volume autoloader_chkpoint").take(1)

autoloader_chkpt=f"/Volumes/{chkpt[0]['catalog']}/{chkpt[0]['database']}/{chkpt[0]['name']}"
print(autoloader_chkpt)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", autoloader_chkpt)
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", autoloader_chkpt)
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

dbutils.fs.cp("/Volumes/dbacademy/labuser14322013_1774521349/bookstore/orders-streaming/02.parquet", "/Volumes/dbacademy/labuser14322013_1774521349/bookstore/orders-raw/02.parquet")

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

dbutils.fs.cp("/Volumes/dbacademy/labuser14322013_1774521349/bookstore/orders-streaming/03.parquet", "/Volumes/dbacademy/labuser14322013_1774521349/bookstore/orders-raw/03.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)