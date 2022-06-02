# Databricks notebook source
# MAGIC %md
# MAGIC #RADS Functions library
# MAGIC Here all the user submitted functions are stored to then be assessed and integrated into the upcoming rads_tools python package (Q3/Q4 2022)

# COMMAND ----------

#This is an example of a standard function with minimal required documentation
def get_latest_ani_table(db_name='scopus', table_name_mask='ani_[\d]{8}'):
  """
  Get the lastest ANI table (table name string) from the `scopus` database, e.g. 
  https://elsevier.cloud.databricks.com/#table/scopus/ani_20220501
  """
  tables = spark.catalog.listTables(dbName=db_name) # list of Table objects
  #finds all table names that fit the mask provided
  ani_table_names = [table.name for table in tables if re.match(table_name_mask, table.name)]
  latest_table_name = sorted(ani_table_names)[-1]
  return latest_table_name
