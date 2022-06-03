# Databricks notebook source
# MAGIC %md
# MAGIC #RADS Functions library
# MAGIC Here all the user submitted functions are stored to then be assessed and integrated into the upcoming rads_tools python package (Q3/Q4 2022)
# MAGIC 
# MAGIC This notebook is linked to the following [git repository](https://github.com/DanielFG-ELS/rads_tools_functions) 

# COMMAND ----------

#This is an example of a standard function with minimal required documentation
def get_latest_table(db_name='scopus', table_name_mask='ani_[\d]{8}'):
  """
  Get the lastest table (table name string) from the provided database and mask, e.g. 
  https://elsevier.cloud.databricks.com/#table/scopus/ani_20220501
  """
  tables = spark.catalog.listTables(dbName=db_name) # list of Table objects
  #finds all table names that fit the mask provided
  ani_table_names = [table.name for table in tables if re.match(table_name_mask, table.name)]
  latest_table_name = sorted(ani_table_names)[-1]
  return latest_table_name

# COMMAND ----------

#function to safely delete files in s3
from random import random
class Remover:
  key = random()
  protected_paths = ['/mnt/els/rads-main/projects','/mnt/els/rads-main/projects/','dbfs:/rads-main/projects','dbfs:/rads-main/projects']
  f = [x for x in path.split('/') if x != '']
  def __init__(self,path,key=key):
    self.key = key
    self.path = path
    print(f"ARE YOU SURE you want to remove {path}? If so run obj.r(path=<your_path>,sure='I AM SURE {key}'")
    
#   def change_key(self):
#     nonlocal key
#     key = 'blah'
    
  def r(self,path,sure):
    print(sure)
    if sure == f'I AM SURE {self.key}' and path not in protected_paths:
      print(f"{path} has been removed.")
      self.key = random()
    elif sure == f'I AM SURE {self.key}' and path in protected_paths or len(f) < 5 or 'rads-main' not in path:
      print(f"You cannot remove {path}")
      self.key = random()
     
    else:
      print('error')
      self.key = random()
      
#     change_key(self)

  def print_key(self):
    print(confirm)
    
  

# COMMAND ----------

#save deliverable to s3
def write_out_deliverable(Project_folder_name,File_name_no_handle,Format,partitions,final_dataframe):
  project_path = '/mnt/els/rads-main/projects/'
  csv_header = False
  if Project_folder_name.strip() == '':
    return 'Please enter the name of the folder'
    
  if File_name_no_handle.strip() == '':
    File_name_no_handle ='File_created_at_' + datetime.strftime(datetime.now(),'%Y%m%d%_H%M%S')
    
  if '.' in File_name_no_handle_no_handle:
    return "Please enter only the name of the file. Format will be used to designate the file handle."
    
  if Format.strip() == '':
    return('Please specify the format in which you want to save the date. Ex: csv, json, parquet')
  if Format.strip().lower() =='csv':
    csv_header = True
  
  SAVE = f"{project_path}{Project_folder_name}/{File_name_no_handle}.{Format}"
  #If we are saving to a csv, we should have a header.  Also, we may want to set partitions to 1 automatically though this is not always the case.
  if csv_header:
    final_dataframe.repartition(partitions).write.format(Format).save(SAVE,header = True)
  else:
    final_dataframe.repartition(partitions).write.format(Format).save(SAVE)
  
  #final_dataframe.repartition(partitions).write.format(Format).save("'"+project_path + Project_folder_name +'/'+File_name_no_handle +'.'+Format+"'"+header)
  

# COMMAND ----------

#fwci for multi subject classification over a dataset, eg ASJC
def func_fwci_multi(df,classification_name):
  df_cits =(
    df
    .withColumnRenamed("EID","Citing")
    .withColumnRenamed("Sort_Year","Cite_Year")
    .withColumn("Cited",func.explode_outer("Citations"))
    .select("Citing","Cited","Cite_Year")
  )
  
  df_cit_joined =(
    df.withColumnRenamed("EID","Cited")
    .join(df_cits,"Cited","left")
    .selectExpr(
      "Cited",
      "sort_year",
      "cite_year",
      "IFNULL((cite_year BETWEEN sort_year AND sort_year+3),False) as CitationInRange_4y",
      "IFNULL((cite_year BETWEEN sort_year AND sort_year+4),False) as CitationInRange_5y"
    )
  )
  
  df_cit_counts =(
    df
    .select(func.col("EID").alias("Cited"),f"{classification_name}","citation_type","sort_year")
    .join(
      df_cit_joined
      .groupBy("Cited")
      .agg(
          #Count Citations in 4 year window, 5 year window, and over all time
          func.sum(func.col('CitationInRange_4y').cast('integer')).alias('Citations_4y'),
          func.sum(func.col('CitationInRange_5y').cast('integer')).alias('Citations_5y'),
          func.count('cite_year').alias('Citations_NoWindow')
      ),"Cited")
  )
  
  
  df_classification_pre_stats =(
    df_cit_counts
    .withColumn(f"{classification_name}",func.explode_outer(f"{classification_name}"))
    .groupBy(f"{classification_name}","Citation_Type","Sort_Year")
    .agg(
      func.sum('Citations_4y').alias('Citations_4y'),
      func.sum('Citations_5y').alias('Citations_5y'),
      func.sum('Citations_NoWindow').alias('Citations_NoWindow')
    )
    .selectExpr(f"{classification_name}","citation_type","sort_year",
                "IF(Citations_4y>0,1,0) as cat_elig_4y",
                "IF(Citations_5y>0,1,0) as cat_elig_5y",
                "IF(Citations_NoWindow>0,1,0) as cat_elig_NoWindow"
               )
  )
  
  df_cit_counts_explode =(
    df_cit_counts
    .withColumn(f"{classification_name}",func.explode_outer(f"{classification_name}"))
    .join(df_classifcation_pre_stats,[f"{classification_name}","citation_type","sort_year"])
    .groupBy("Cited")
    .agg(
      func.sum('cat_elig_4y').alias(f'{classification_name}_count_4y'),
      func.sum('cat_elig_5y').alias(f'{classification_name}_count_5y'),
      func.sum('cat_elig_NoWindow').alias(f'{classification_name}_count_NoWindow'),
      # just so that we keep these values, they are static in the group anyway.
      func.first('Citations_4y').alias('Citations_4y'),
      func.first('Citations_5y').alias('Citations_5y'),
      func.first('Citations_NoWindow').alias('Citations_NoWindow'),
      func.first('citation_type').alias('citation_type'),
      func.first('sort_year').alias('sort_year'),
      # recollect the exploded classifications so that we can explode again later. alternative is a join later, but we need to get the aggregated classification_count per window into the mix anyway.
      func.collect_list(f"{classification_name}").alias(f"{classification_name}")
    )
    .selectExpr('*',
               f'1.0/{classification_name}_count_4y as fraction_4y',
               f'1.0/{classification_name}_count_5y as fraction_5y',
               f'1.0/{classification_name}_count_NoWindow as fraction_NoWindow',
               f'Citations_4y/{classification_name}_count_4y as fraction_cits_4y',
               f'Citations_5y/{classification_name}_count_5y as fraction_cits_5y',
               f'Citations_NoWindow/{classification_name}_count_NoWindow as fraction_cits_NoWindow')\
  .withColumn(f"{classification_name}",func.explode_outer(f"{classification_name}"))
  )
  
  df_ascj_doctype_pubyear_cites =(
    df_cit_counts_explode
    .groupBy(f"{classification_name}","citation_type","sort_year")
    .agg(
      func.sum('fraction_4y').alias('frac_documents_4y'),
      func.sum('fraction_5y').alias('frac_documents_5y'),
      func.sum('fraction_NoWindow').alias('frac_documents_NoWindow'),
      func.sum('fraction_cits_4y').alias('frac_citations_4y'),
      func.sum('fraction_cits_5y').alias('frac_citations_5y'),
      func.sum('fraction_cits_NoWindow').alias('frac_citations_NoWindow')
    )
    .selectExpr("*",
                'IFNULL(frac_documents_4y/frac_citations_4y,0) as inv_cpp_4y',
                'IFNULL(frac_documents_5y/frac_citations_5y,0) as inv_cpp_5y',
                'IFNULL(frac_documents_NoWindow/frac_citations_NoWindow,0) as inv_cpp_NoWindow'
               )
  )
                
  df_fwci =(
    df_cit_counts_explode
    .join(df_classification_doctype_pubyear_cites,[f"{classification_name}","citation_type","sort_year"])
    .groupBy("cited")
    .agg(
      func.first('sort_year').alias('sort_year'),
      func.first('Citations_4y').alias('Citations_4y'),
      func.first('Citations_5y').alias('Citations_5y'),
      func.first('Citations_NoWindow').alias('Citations_NoWindow'),
      func.first(f'{classification_name}_count_4y').alias(f'{classification_name}_count_4y'),
      func.first(f'{classification_name}_count_5y').alias(f'{classification_name}_count_5y'),
      func.first(f'{classification_name}_count_NoWindow').alias(f'{classification_name}_count_NoWindow'),
    # sum inverse cpp. We will divide the ASJC_count for each document by the sum of the inverse cpp's of the documents' subjects. That will be the "expected" citations.
      func.sum('inv_cpp_4y').alias('sum_inv_cpp_4y'),
      func.sum('inv_cpp_5y').alias('sum_inv_cpp_5y'),
      func.sum('inv_cpp_NoWindow').alias('sum_inv_cpp_NoWindow')
    )
    .selectExpr(
      '*',
      f'{classification_name}_count_4y/sum_inv_cpp_4y as Expected_4y',
      f'{classification_name}_count_5y/sum_inv_cpp_5y as Expected_5y',
      f'{classification_name}_count_NoWindow/sum_inv_cpp_NoWindow as Expected_NoWindow'
    )
    .selectExpr(
      '*',
      'Citations_4y/Expected_4y as FWCI_4y',
      'Citations_5y/Expected_5y as FWCI_5y',
      'Citations_NoWindow/Expected_NoWindow as FWCI_NoWindow'
    )
    .drop('sum_inv_cpp_4y','sum_inv_cpp_5y','sum_inv_cpp_NoWindow')
    .withColumnRenamed("Cited","EID")
    )
  
  return df_fwci

# COMMAND ----------

#fwci for single subject classification over a dataset, eg SM subfields
def func_fwci_multi(df,classification_name):
  df_cits =(
    df
    .withColumnRenamed("EID","Citing")
    .withColumnRenamed("Sort_Year","Cite_Year")
    .withColumn("Cited",func.explode_outer("Citations"))
    .select("Citing","Cited","Cite_Year")
  )
  
  df_cit_joined =(
    df.withColumnRenamed("EID","Cited")
    .join(df_cits,"Cited","left")
    .selectExpr(
      "Cited",
      "sort_year",
      "cite_year",
      "IFNULL((cite_year BETWEEN sort_year AND sort_year+3),False) as CitationInRange_4y",
      "IFNULL((cite_year BETWEEN sort_year AND sort_year+4),False) as CitationInRange_5y"
    )
  )
  
  df_cit_counts =(
    df
    .select(func.col("EID").alias("Cited"),f"{classification_name}","citation_type","sort_year")
    .join(
      df_cit_joined
      .groupBy("Cited")
      .agg(
          #Count Citations in 4 year window, 5 year window, and over all time
          func.sum(func.col('CitationInRange_4y').cast('integer')).alias('Citations_4y'),
          func.sum(func.col('CitationInRange_5y').cast('integer')).alias('Citations_5y'),
          func.count('cite_year').alias('Citations_NoWindow')
      ),"Cited")
  )
  
  
  df_classification_pre_stats =(
    df_cit_counts
    .groupBy(f"{classification_name}","Citation_Type","Sort_Year")
    .agg(
      func.sum('Citations_4y').alias('Citations_4y'),
      func.sum('Citations_5y').alias('Citations_5y'),
      func.sum('Citations_NoWindow').alias('Citations_NoWindow')
    )
    .selectExpr(f"{classification_name}","citation_type","sort_year",
                "IF(Citations_4y>0,1,0) as cat_elig_4y",
                "IF(Citations_5y>0,1,0) as cat_elig_5y",
                "IF(Citations_NoWindow>0,1,0) as cat_elig_NoWindow"
               )
  )
  
  df_cit_counts_explode =(
    df_cit_counts
    .join(df_classifcation_pre_stats,[f"{classification_name}","citation_type","sort_year"])
    .groupBy("Cited")
    .agg(
      func.sum('cat_elig_4y').alias(f'{classification_name}_count_4y'),
      func.sum('cat_elig_5y').alias(f'{classification_name}_count_5y'),
      func.sum('cat_elig_NoWindow').alias(f'{classification_name}_count_NoWindow'),
      # just so that we keep these values, they are static in the group anyway.
      func.first('Citations_4y').alias('Citations_4y'),
      func.first('Citations_5y').alias('Citations_5y'),
      func.first('Citations_NoWindow').alias('Citations_NoWindow'),
      func.first('citation_type').alias('citation_type'),
      func.first('sort_year').alias('sort_year'),
    )
    .selectExpr('*',
               f'1.0/{classification_name}_count_4y as fraction_4y',
               f'1.0/{classification_name}_count_5y as fraction_5y',
               f'1.0/{classification_name}_count_NoWindow as fraction_NoWindow',
               f'Citations_4y/{classification_name}_count_4y as fraction_cits_4y',
               f'Citations_5y/{classification_name}_count_5y as fraction_cits_5y',
               f'Citations_NoWindow/{classification_name}_count_NoWindow as fraction_cits_NoWindow')\
  .withColumn(f"{classification_name}",func.explode_outer(f"{classification_name}"))
  )
  
  df_ascj_doctype_pubyear_cites =(
    df_cit_counts_explode
    .groupBy(f"{classification_name}","citation_type","sort_year")
    .agg(
      func.sum('fraction_4y').alias('frac_documents_4y'),
      func.sum('fraction_5y').alias('frac_documents_5y'),
      func.sum('fraction_NoWindow').alias('frac_documents_NoWindow'),
      func.sum('fraction_cits_4y').alias('frac_citations_4y'),
      func.sum('fraction_cits_5y').alias('frac_citations_5y'),
      func.sum('fraction_cits_NoWindow').alias('frac_citations_NoWindow')
    )
    .selectExpr("*",
                'IFNULL(frac_documents_4y/frac_citations_4y,0) as inv_cpp_4y',
                'IFNULL(frac_documents_5y/frac_citations_5y,0) as inv_cpp_5y',
                'IFNULL(frac_documents_NoWindow/frac_citations_NoWindow,0) as inv_cpp_NoWindow'
               )
  )
                
  df_fwci =(
    df_cit_counts_explode
    .join(df_classification_doctype_pubyear_cites,[f"{classification_name}","citation_type","sort_year"])
    .groupBy("cited")
    .agg(
      func.first('sort_year').alias('sort_year'),
      func.first('Citations_4y').alias('Citations_4y'),
      func.first('Citations_5y').alias('Citations_5y'),
      func.first('Citations_NoWindow').alias('Citations_NoWindow'),
      func.first(f'{classification_name}_count_4y').alias(f'{classification_name}_count_4y'),
      func.first(f'{classification_name}_count_5y').alias(f'{classification_name}_count_5y'),
      func.first(f'{classification_name}_count_NoWindow').alias(f'{classification_name}_count_NoWindow'),
    # sum inverse cpp. We will divide the ASJC_count for each document by the sum of the inverse cpp's of the documents' subjects. That will be the "expected" citations.
      func.sum('inv_cpp_4y').alias('sum_inv_cpp_4y'),
      func.sum('inv_cpp_5y').alias('sum_inv_cpp_5y'),
      func.sum('inv_cpp_NoWindow').alias('sum_inv_cpp_NoWindow')
    )
    .selectExpr(
      '*',
      f'{classification_name}_count_4y/sum_inv_cpp_4y as Expected_4y',
      f'{classification_name}_count_5y/sum_inv_cpp_5y as Expected_5y',
      f'{classification_name}_count_NoWindow/sum_inv_cpp_NoWindow as Expected_NoWindow'
    )
    .selectExpr(
      '*',
      'Citations_4y/Expected_4y as FWCI_4y',
      'Citations_5y/Expected_5y as FWCI_5y',
      'Citations_NoWindow/Expected_NoWindow as FWCI_NoWindow'
    )
    .drop('sum_inv_cpp_4y','sum_inv_cpp_5y','sum_inv_cpp_NoWindow')
    .withColumnRenamed("Cited","EID")
    )
  
  return df_fwci

# COMMAND ----------

# MAGIC %md ## Jeroens functions

# COMMAND ----------

# MAGIC %md ### file_exists(path)
# MAGIC function used to check if a folder or file exists in dbfs

# COMMAND ----------

def file_exists(path):
  if path[:5] == "/dbfs":
    import os
    return os.path.exists(path)
  else:
    try:
      dbutils.fs.ls(path)
      return True
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        return False
      else:
        raise

# COMMAND ----------

# MAGIC %md ### nunllsafeflatten(nested_array)
# MAGIC a replacement for func.flatten that takes out NULLs so that it doesn't return None when supplying [[x],None,[y]] but [x,y]

# COMMAND ----------

nullsafeflatten=lambda x: func.flatten(func.array_except(x,func.array(func.lit(None))))

# COMMAND ----------

# MAGIC %md ### get_citation_type_df()
# MAGIC returns a datadframe that can be used to translate a citation type code to its full name equivalent

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import Row
def get_citation_type_df():
  citation_types=[
    ['ar','Article'],
    ['re','Review'],
    ['cp','Conference Paper'],
    ['bk','Book'],
    ['ch','Chapter'],
    ['ab','Abstract Report'],
    ['ba','Chapter with Book Abstracts'],
    ['br','Book Review'],
    ['bz','Business Article'],
    ['cb','Conference Abstract'],
    ['cr','Conference Review'],
    ['di','Dissertation'],
    ['dp','Data Paper'],
    ['ed','Editorial'],
    ['er','Erratum'],
    ['ip','Article in Press [*]'], # this citation type should dissapear as IP status in now captured in publication stage.
    ['le','Letter'],
    ['mm','Multimedia'],
    ['no','Note'],
    ['pa','Patent'],
    ['pp','Preprint'],
    ['pr','Press Release'],
    ['rf','Chapter with References Only'],
    ['rp','Report'],
    ['sh','Short Survey'],
    ['tb','Tombstone'],
    ['wp','Working Paper']
  ]
  return (spark.createDataFrame(list(map(lambda x: Row(citation_type=x[0],DocumentType=x[1]), citation_types))))

# COMMAND ----------

# MAGIC %md ### add_author_affiliation_formatted(df_ani)
# MAGIC Adds 2 columns to df_ani:
# MAGIC * affliation_flat: the Af, but with some restructured columns, as well as (important!) the affiliation_idx (sequence of the affiliation)
# MAGIC * authors: the Au, but with some restructured columns, as well as each of the affiliation_flat that the author is affiliated with on the paper
# MAGIC 
# MAGIC Warning: this function is quite expensive and slow on large datasets

# COMMAND ----------

def add_author_affiliation_formatted(df):
  return (
    df
    .withColumn(
      'affiliation_flat',
      func.transform(
        func.col('Af'),
        lambda x_af,x_af_idx: func.struct(
          func.filter(
            func.concat(
              func.array(x_af.affiliation_text),
              x_af.affiliation_organization,
            ),
            lambda x : (x.isNotNull())
          ).alias('affiliation_organization'),
          func.filter(
            func.concat(
              func.array(x_af.affiliation_address_part),
              func.array(x_af.affiliation_postal_code),
              func.array(x_af.affiliation_city),
              func.array(x_af.affiliation_city_group),
              func.array(x_af.affiliation_state)
            ),
            lambda x : (x.isNotNull())
          ).alias('affiliation_address'),
          x_af.affiliation_tag_country.alias('country_code'), 
          x_af.affiliation_ids.alias('affiliation_ids'),
          (x_af_idx+1).alias('affiliation_idx')
        )
      )
    )
    .withColumn(
      'authors',
      # filter affiliation_flat by the af indexes from the au_af array for the author with this sequence
      # then produce a struct with the author details, and its affiliations.
      func.transform(
        func.col('Au'),
        lambda Au: (
          func.struct(
            Au.given_name.alias('given_name'),
            Au.surname.alias('family_name'),
            Au.auid.alias('author_id'),
            Au.Authorseq.alias('author_position'), 
            func.filter(
              func.col('affiliation_flat'),
              lambda af,afidx : (
                func.array_contains(
                  func.transform(
                    func.filter(
                      func.col('Au_af'),
                      lambda Au_af : Au_af.Authorseq==Au.Authorseq
                    ),
                    lambda x: (x.affiliation_seq-1)
                  ),
                  afidx
                )
              )
            ).alias('affiliations')
          )
        )
      )
    )
  )

# COMMAND ----------

# MAGIC %md ### array_join_clean_pipe(array)
# MAGIC returns and array joined by pipe, removing any pipes from the input. Useful for e.g. exporting a list field into a CSV

# COMMAND ----------

# function to join an array-field (implode)
# we clean the strings before imploding so that there is no pipe in the string
# then implode using the pipe character
# also ignore empty strings (filter)
def array_join_clean_pipe(colfield):
  return (
    func.array_join(func.filter(func.transform(colfield,lambda x: func.trim(func.regexp_replace(x.cast('string'),'\|',''))),lambda y: func.length(y)>0),'|')
  )

# COMMAND ----------

# MAGIC %md ### normalize_doi(doi_string_column)
# MAGIC Clean dois. great for matching scopus and other sources by doi.

# COMMAND ----------

normalize_doi=lambda doi_col: (
  func.regexp_replace(
    func.lower(doi_col),
    '([\\u00A0]+)|(^doi:)|(http[s]?://[^/]+/)|"|\*|[\s]+',''
    # removing, in order of appearance:
    # - non breaking spaces
    # - lead doi: text
    # - a url-domain prefix
    # - double quote
    # - normal spaces
  )
)
