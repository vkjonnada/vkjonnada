import sys
import configparser
import string
import boto3
import json
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType,DoubleType,DateType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','database','TableName','S3BucketName','schema_file','S3KmsKey'])

## Initialize pyspark ##
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Assign job arguments to variables ##
bucket =args['S3BucketName']
Table_Name=args['TableName'].lower()
Database = args['database'].lower()
print ('\n-- S3BucketName: '+ args['S3BucketName'])
print ('\nRunning against database: ' + args['database'])
print ('\n-- tableName: '+ args['TableName'])

## Initialize boto3 client for s3 and athena##
s3 = boto3.resource('s3')
s3_bucket = s3.Bucket(bucket)
client = boto3.client('athena', region_name='us-east-1')
glue = boto3.client(service_name='glue',region_name='us-east-1',endpoint_url='https://glue.us-east-1.amazonaws.com')
## Read parameters from configuration file ##
config = configparser.ConfigParser()
config.read(['config.ini'])
partition_config = dict(config.items('PARTITION_CONFIG'))
path_config = dict(config.items('PATH'))
partition_key =partition_config[Table_Name]
partition_key1= partition_key.replace("'", "")
partition_key_list = partition_key1.split (",")

## List of dimension jobs for which there are no hash files. Replace hash file step will be skipped for these jobs ##
no_hash_job_list=['dim_account_history_tbl']

## List of fact job for which MSCK REPAIR TABLE will be used to create partitions ##
partition_repair_fact_job_list=['fact_investment_election_tbl','fact_proaccount_election_tbl','fact_plan_fee_criteria_tbl']

## List of dim jobs which are not full refresh. Data will not be loaded to the target table if validation fails for these jobs. ##
non_full_refresh_dim_job_list=['dim_account_history_tbl','dim_fin_transaction_code_tbl']

if (Table_Name[:4] == 'fact'):
	## Load the fact parameters from the config file ##
    path_to_read_fact  = path_config['fact_working_dir']
    path_to_upload_fact  = path_config['fact_dir']
    path_to_read_schema = path_config['fact_schema_dir']
    
	## Load the schema for the fact file from the schema file ##
    schema_file = "s3://"+ bucket + path_to_read_schema + args['schema_file']
    schema_json = spark.read.text(schema_file).first()[0]
    CustomSchema = StructType.fromJson(json.loads(schema_json))
    
	## Data frame to read data from fact delta csv file ##
    df_fact_delta = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").schema(CustomSchema).load("s3://"+ bucket + "/" + args['database'] + "/" + path_to_read_fact + args['TableName'])
    
	## Load list of partition_key values from fact delta file ###
    partition_val_list = df_fact_delta.selectExpr(partition_key).distinct()
	
    path_s3=args['database'] + path_to_upload_fact + args['TableName']+"/"
    
    print("path_s3:"+path_s3)
    objs = list(s3_bucket.objects.filter(Prefix=path_s3))
    size=len(objs)
    if ((size!=0) & (Table_Name!= 'fact_non_financial_activity_tbl')):
	    ## Data frame to read data from fact parquest file ##
        tbl_fact = spark.read.parquet("s3://"+ bucket + "/" + args['database'] + "/" + path_to_upload_fact + args['TableName'])
	    
	    ## Load list of parititon keys from the fact delta file which are present in the target fact parquet file ##
        partition_del_list = tbl_fact.alias("fact").selectExpr(partition_key).distinct().join(partition_val_list.alias("delta"),col("fact."+partition_key)==col("delta."+partition_key),how="inner").selectExpr("delta."+partition_key).distinct()
        
	    ## Delete the partition folders and files which are already present in the target fact parquet file (identfied in the previous step) ##
        for partition_value in partition_del_list.collect():
            part_val = str(partition_value[0])
            print("Processing partition:"+part_val)
            folder_to_delete = args['database'] + path_to_upload_fact + args['TableName'] + "/" + partition_key + '=' + part_val + "/"
            file_to_delete = args['database'] + path_to_upload_fact + args['TableName'] + "/" + partition_key + '=' + part_val + "_$folder$"
            s3.Object(bucket, file_to_delete).delete()
            s3_bucket.objects.filter(Prefix=folder_to_delete).delete()

	## Append the data from fact delta csv file to the target fact parquet file ##
    df_fact_delta.write.partitionBy(partition_key1).mode("append").parquet("s3://"+ bucket + "/" + args['database'] + "/" + path_to_upload_fact + args['TableName'] , compression = 'gzip')

    if (Table_Name not in partition_repair_fact_job_list):
        ## Generate alter scripts for creating new paritions for each parition key in the fact delta file and execute it using athena client ##
        for partition_value in partition_val_list.collect():
            part_val = str(partition_value[0])
            location = '\'s3://' + bucket + "/" + args['database'] + path_to_upload_fact + args['TableName'] + "/" + partition_key + '=' + part_val + '/\''
            sql = 'alter table ' + args['TableName'] + ' add if not exists partition(' + partition_key + '=\'' + part_val + '\') location' + location 
            print('\nAdding Partitions:' +sql)
            config = {
            'OutputLocation': 's3://' + bucket + "/" + args['database'] + '/query_logs/FACT/' + args['TableName'] ,
            'EncryptionConfiguration': {'EncryptionOption': 'SSE_KMS', 'KmsKey':args['S3KmsKey']}}
            context = {'Database': Database}
            client.start_query_execution(QueryString = sql,QueryExecutionContext = context,ResultConfiguration = config)
    else:
        sql = 'MSCK REPAIR TABLE ' + args['TableName']
        print('\nRefreshing all Partitions:' +sql)
        config = {
        'OutputLocation': 's3://' + bucket + "/" + args['database'] + '/query_logs/FACT/' + args['TableName'] ,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_KMS', 'KmsKey':args['S3KmsKey']}}
        context = {'Database': Database}
        client.start_query_execution(QueryString = sql,QueryExecutionContext = context,ResultConfiguration = config)

if (Table_Name[:3] == 'dim'):
	## Load the dimension parameters from the config file ##
    path_to_read_dim  = path_config['dimension_working_dir']
    path_to_upload_dim  = path_config['dimension_dir']
    path_to_read_schema = path_config['dimension_schema_dir']
    path_to_dim_debug = path_config['dimension_debug_dir']
    hash_dir = path_config['hash_dir']
    hash_working_dir  = path_config['hash_working_dir']
    unique_key_columns = dict(config.items('UNIQUE_KEY_COLUMNS'))
    unique_col_list = unique_key_columns[Table_Name]
    unique_columns_list = unique_col_list.split (",")

    dim_key_column = dict(config.items('DIM_KEY_COLUMN'))[Table_Name].split (",")

	## Load the schema for the dimension file from the schema file ##
    schema_file = "s3://"+ bucket + path_to_read_schema  + args['schema_file']
    schema_json = spark.read.text(schema_file).first()[0]
    CustomSchema = StructType.fromJson(json.loads(schema_json))
    
    ## Data frame to read data from dimension delta csv file ##
    df_dim = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").schema(CustomSchema).load("s3://"+ bucket + "/" + args['database'] + "/" + path_to_read_dim + args['TableName'])

    ## START OF VALIDATIONS ##
    # Writing dimension staging data as parquet format to DEBUG directory
    df_dim.write.mode("overwrite").parquet("s3://"+ bucket + "/" + args['database'] + "/" + path_to_dim_debug + args['TableName'], compression = 'gzip')    
    #Group by all_cols and filter rows  that are exact duplicate with all columns 
    df_all_cols_dup = df_dim.groupBy(df_dim.columns).count().filter(column('count') > 1)
    #Group by columns that uniquely identify the rows in Dimensiona and filter rows that are duplicate 
    df_key_cols_dup = df_dim.groupBy(*unique_columns_list).count().filter(column('count') > 1)

    #Validation for duplicates based on unique column list
    if (df_key_cols_dup.count() > 0):
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

	## Replace the target fact parquet file data with data from dim delta csv file ##
    df_dim.write.partitionBy(partition_key1).mode("overwrite").parquet("s3://"+ bucket + "/" + args['database'] + "/" + path_to_upload_dim + args['TableName'], compression = 'gzip')
	
	## Data frame to read data from dimension hash file from working directory to hash file directory ##
    if (Table_Name not in no_hash_job_list):
        df_hash = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").option('compression', 'gzip').load("s3://"+ bucket + "/" + args['database'] + "/" + hash_working_dir + args['TableName'] +"_HASH")
        df_hash_distinct = df_hash.distinct()
        df_hash_distinct.write.mode("overwrite").format('com.databricks.spark.csv').option('delimiter','|').option("header", "true").option('compression', 'gzip').save("s3://"+ bucket + "/" + args['database'] + "/" + hash_dir + args['TableName']+"_HASH")
        
	## Load list of partition_key values from dimension delta file ###
    partition_val_list = df_dim.selectExpr(partition_key).distinct()
	
	## Generate alter scripts for creating new paritions for each parition key in the dimension delta file and execute it using athena client ##
    for partition_value in partition_val_list.collect():
        print(partition_value)
        part_val = str(partition_value[0])
        location = '\'s3://' + bucket + "/" +  args['database']  + path_to_upload_dim + args['TableName'] + "/" + partition_key + '=' + part_val + '/\''
        sql = 'alter table ' + args['TableName'] + ' add if not exists partition(' + partition_key + '=\'' + part_val + '\') location' + location
        print('\nAdding Partitions:' +sql)
        config = {
        'OutputLocation': 's3://' + bucket + "/" + args['database'] + '/query_logs/DIMENSION/' + args['TableName'] ,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_KMS', 'KmsKey':args['S3KmsKey']}}
        context = {'Database': Database}
        client.start_query_execution(QueryString = sql,QueryExecutionContext = context,ResultConfiguration = config)
job.commit()
