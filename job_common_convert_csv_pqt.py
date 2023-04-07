import sys
import configparser
import string
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType,DoubleType,DateType
import json
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','database','TableName','S3BucketName','schema_file'])
database=args['database']
bucket =args['S3BucketName']
Table_Name=args['TableName'].lower()
schema_file=args['schema_file']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config = configparser.ConfigParser()
config.read(['config.ini'])

path_config = dict(config.items('PATH'))

bucket =args['S3BucketName']
print ('\n-- S3BucketName: '+ args['S3BucketName'])
print ('\nRunning against database: ' + args['database'])
print ('\n-- tableName: '+ args['TableName'])
s3 = boto3.resource('s3')

no_hash_job_list=['dim_external_interest_rates_tbl','dim_age_group_tbl']

## List of dim jobs which are not full refresh. Data will not be loaded to the target table if validation fails for these jobs. ##
non_full_refresh_dim_job_list=['dim_account_history_tbl','dim_fin_transaction_code_tbl']

if (Table_Name[:4] == 'fact'):
    path_to_read_fact  = path_config['fact_working_dir']
    path_to_upload_fact  = path_config['fact_dir']
    path_to_read_schema = path_config['fact_schema_dir']
    
    schema_file = "s3://" + bucket + path_to_read_schema  + args['schema_file']
    schema_json = spark.read.text(schema_file).first()[0]
    CustomSchema = StructType.fromJson(json.loads(schema_json))
    
    compression_type = 'gzip'
    df_fact = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").schema(CustomSchema).load("s3://"+ bucket + "/" + args['database'] + "/" +  path_to_read_fact + args['TableName'])
    df_fact.write.mode("append").format('parquet').option('compression', compression_type).save("s3://"+ bucket + "/" + args['database'] +"/" + path_to_upload_fact + args['TableName'])
    
if (Table_Name[:3] == 'dim'):
    path_to_read_dim  = path_config['dimension_working_dir']
    path_to_upload_dim  = path_config['dimension_dir']
    path_to_read_schema = path_config['dimension_schema_dir']
    hash_dir = path_config['hash_dir']
    hash_working_dir  = path_config['hash_working_dir']
    path_to_dim_debug = path_config['dimension_debug_dir']
    unique_key_columns = dict(config.items('UNIQUE_KEY_COLUMNS'))
    unique_col_list = unique_key_columns[Table_Name]
    unique_columns_list = unique_col_list.split (",")
    dim_key_column = dict(config.items('DIM_KEY_COLUMN'))[Table_Name].split (",")	

    schema_file = "s3://"+ bucket + path_to_read_schema + args['schema_file']
    schema_json = spark.read.text(schema_file).first()[0]
    CustomSchema = StructType.fromJson(json.loads(schema_json))
    
    compression_type = 'gzip'
    df_dim = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").schema(CustomSchema).load("s3://"+ bucket + "/" + args['database'] + "/" + path_to_read_dim + args['TableName'])

    ## START OF VALIDATIONS ##
    # Writing dimension staging data as parquet format to DEBUG directory
    df_dim.write.mode("overwrite").format('parquet').option('compression', compression_type).save("s3://"+ bucket + "/" + args['database'] + "/" + path_to_dim_debug + args['TableName'])

    #Group by all_cols and filter rows  that are exact duplicate with all columns 
    df_all_cols_dup = df_dim.groupBy(df_dim.columns).count().filter(column('count') > 1)
    #Group by columns that uniquely identify the rows in Dimensiona and filter rows that are duplicate 
    df_key_cols_dup = df_dim.groupBy(*unique_columns_list).count().filter(column('count') > 1)

    #Validation for duplicates based on unique column list
    if ((Table_Name!= 'dim_external_interest_rates_tbl') and (df_key_cols_dup.count() > 0)):
     #if the rows are exact duplicate , apply distinct() 
        if (df_key_cols_dup.count() == df_all_cols_dup.count()):
            df_dim = df_dim.distinct()
            print("Following duplicates are removed :",df_key_cols_dup.collect())
        else :
            # kill the job with error message with the list of rows thats causes discrepancy 
            raise Exception("Duplicate Rows for the following records .Please fix before rerunning ",df_key_cols_dup.collect())

    #Group by all records based on dimension key and check if there are more than one record with the same dimension key
    df_dim_key_dup = df_dim.groupBy(*dim_key_column).count().filter(column('count') > 1)
    if ((df_dim_key_dup.count() > 0) and (Table_Name!='dim_plan_employer_paycenter_tbl')):
        raise Exception("Duplicate dimension keys for the following records .Please fix before rerunning ",df_dim_key_dup.collect())
 
    ## END OF VALIDATIONS ##

    df_dim.write.mode("overwrite").format('parquet').option('compression', compression_type).save("s3://"+ bucket + "/" + args['database'] + "/" + path_to_upload_dim + args['TableName'])

    ## Data frame to read data from dimension hash file from working directory to hash file directory ##
    if (Table_Name not in no_hash_job_list):
        df_hash = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").option('compression', 'gzip').load("s3://"+ bucket + "/" + args['database'] + "/" + hash_working_dir + args['TableName'] +"_HASH")
        df_hash_distinct = df_hash.distinct()
        df_hash.write.mode("overwrite").format('com.databricks.spark.csv').option('delimiter','|').option("header", "true").option('compression', 'gzip').save("s3://"+ bucket + "/" + args['database'] + "/" + hash_dir + args['TableName']+"_HASH")
    
job.commit()
