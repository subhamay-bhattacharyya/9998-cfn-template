import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
 
 
 
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name,explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df
     
def flatten(df):
  read_nested_json_flag = True
  while read_nested_json_flag:
    df = read_nested_json(df);
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True;
  return df;
 
def main():
    ## @params: [JOB_NAME]
    spark = SparkSession.builder.getOrCreate()
    args = getResolvedOptions(sys.argv, ["source_bucket_name","source_file_name","target_file_path"])
    source_bucket_name=args['source_bucket_name']
    source_file_name=args['source_file_name']
    target_file_path=args['target_file_path']
 
    input_file_path=f"s3a://{source_bucket_name}/{source_file_name}"
    target_folder = (source_file_name.split('.')[0]).split('/')[-1]
 
 
    print(f"Bucket Name : {source_bucket_name}")
    print(f"File Name : {source_file_name}")
    print(f"Input File Path : {input_file_path}")
    print(f"Target File Path : {target_file_path}")
     
    df = spark.read.option("multiline", True).option("inferSchema", False).json(input_file_path)
    df_flattened =  flatten(df)
    df_flattened.coalesce(1).write.format("csv").option("header", "true").save(f"s3a://{target_file_path}/{target_folder}/")
 
main()