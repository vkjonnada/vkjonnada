import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import functools
import configparser

## Initialize pyspark ##
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## Read parameters from configuration file   ##
config = configparser.ConfigParser()
config.read("config.ini")
path = dict(config.items('PATH'))
dcowner_master_dir = path['dcowner_master_dir']
dimension_working_dir = path['dimension_working_dir']
hash_working_dir = path['hash_working_dir']
dimension_dir = path['dimension_dir']
hash_dir = path['hash_dir']

## Assign job arguments to variables ##
args = getResolvedOptions(sys.argv, ['database','TableName','S3BucketName'])
bucket = args['S3BucketName']
database = args['database']
Table_Name=args['TableName'].lower()
dimension = dict(config.items('DIMENSION_TABLE'))
output_file = dimension[Table_Name]

hash_columns = dict(config.items('HASH_COLUMNS'))
hash_col_list = hash_columns[Table_Name]

hash_columns_list = hash_col_list.split (",")

tbl_cntrl_calendar = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "CNTRL_CALENDAR")

#read the hash file from master directory
tbl_hash = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + hash_dir + output_file + "_HASH")

df_cntrl_calendar =  tbl_cntrl_calendar.selectExpr("CAST(date_format(calendar_date,'yyyyMMdd') as INTEGER) as date_key",
                                                   "calendar_date",
                                                   "week_start_date",
                                                   "week_end_date",
                                                   "date_format(calendar_date, 'EEEE') as week_day_long_name",
                                                   "date_format(calendar_date, 'E') as week_day_short_name",                                                 
                                                   "weekday_ind",
                                                   "holiday_ind",
                                                   "holiday_name",
                                                   "last_business_day_of_month_ind",
                                                   "monthend_weekend_ind",
                                                   "nationwide_open_ind",
                                                   "nyse_open_ind",
                                                   "dayofweek(calendar_date) as day_num_of_week",
                                                   "dayofmonth(calendar_date) as day_num_of_month",
                                                   "dayofyear(calendar_date) as day_num_of_year",
                                                   "upper(concat(date_format(calendar_date, 'MMM'),'-',year(calendar_date))) as month_id",
                                                   "date_format(calendar_monthend_date, 'd') as days_in_month",
                                                   "trunc(calendar_monthend_date, 'month') AS month_start_date",  
                                                   "calendar_monthend_date AS month_end_date",
                                                   "date_format(calendar_date, 'MMM') AS month_short_name",
                                                   "date_format(calendar_date, 'MMMM') AS month_long_name",
                                                   "date_format(calendar_date, 'M') AS month_num_of_year",
                                                   "concat('Q', quarter(calendar_date),'-', year(calendar_date)) AS quarter_id",
                                                   "(datediff((quarter_end_date),(quarter_start_date)) + 1) AS days_in_quarter",
                                                   "quarter_start_date",
                                                   "quarter_end_date",
                                                   "quarter(calendar_date) AS quarter_num_of_year",
                                                   "concat('H', CASE WHEN quarter(calendar_date) <= 2 THEN 1 ELSE 2 END, '-', year(calendar_date)) AS semi_annual_id",
                                                   "(datediff((semi_annual_end_date),(semi_annual_start_date)) + 1) AS days_in_semi_annual",
                                                   "semi_annual_start_date",
                                                   "semi_annual_end_date",
                                                   "CASE WHEN quarter(calendar_date) <= 2 THEN 1 ELSE 2 END AS semi_annual_num_of_year",
                                                   "calendar_year",
                                                   "calendar_number_of_days AS days_in_year",
                                                   "annual_start_date",
                                                   "annual_end_date",
                                                   #"weekofyear(calendar_date) AS week_num_of_year"
                                                   "CASE WHEN weekofyear(calendar_date) =1 AND date_format(calendar_date, 'M') =12 THEN 53  WHEN weekofyear(calendar_date) =53 AND date_format(calendar_date, 'M') =1 THEN 1 ELSE weekofyear(calendar_date)  END AS week_num_of_year"
                                                  )

 ### Generating New Hash file and Target file

#get the highest key_val
#max_date_key=tbl_hash.agg(max(col("DATE_KEY"))).collect()[0][0]

#calculate hash value for the data in staging file
df_date_hash=df_cntrl_calendar.withColumn("HASH_VALUE",sha2(concat_ws("||",*hash_columns_list),512))

#join staging data frame  with existing old hash file to get the Keys for existing records and generate new keys for new inserts
new_master_dim_date=df_date_hash.alias("fnd").join(tbl_hash.alias("h").filter(col("h.calendar_date").isNotNull()),to_date(col("fnd.calendar_date"))==to_date(col("h.calendar_date")),how='left').selectExpr("fnd.*","NVL(h.create_date,current_timestamp()) as CREATE_DATE","CASE WHEN (h.calendar_date IS NULL OR (h.HASH_VALUE<>fnd.HASH_VALUE)) THEN current_timestamp() ELSE h.LAST_UPDATE_DATE END as LAST_UPDATE_DATE")
new_master_dim_date.persist()

#reordering columns and setting default values as per DDL
dim_date_final = new_master_dim_date.selectExpr("date_key",
"calendar_date",
"week_start_date",
"week_end_date",
"week_day_long_name",
"week_day_short_name",
"weekday_ind",
"holiday_ind",
"holiday_name",
"last_business_day_of_month_ind",
"monthend_weekend_ind",
"nationwide_open_ind",
"nyse_open_ind",
"day_num_of_week",
"day_num_of_month",
"day_num_of_year",
"month_id",
"days_in_month",
"month_start_date",
"month_end_date",
"month_short_name",
"month_long_name",
"month_num_of_year",
"quarter_id",
"days_in_quarter",
"quarter_start_date",
"quarter_end_date",
"quarter_num_of_year",
"semi_annual_id",
"days_in_semi_annual",
"semi_annual_start_date",
"semi_annual_end_date",
"semi_annual_num_of_year",
"calendar_year",
"days_in_year",
"annual_start_date",
"annual_end_date",
"week_num_of_year",
"create_date",
"last_update_date")

#generate new hash file with old and new keys and updated hash value
new_hash_dim_date=new_master_dim_date.selectExpr("DATE_KEY","CALENDAR_DATE","CREATE_DATE","LAST_UPDATE_DATE","HASH_VALUE").orderBy(["DATE_KEY"], ascending=[1])

new_master_dim_date_null_filtered = (functools.reduce(lambda result, col_name: result.withColumn(col_name, when(col(col_name).isNull(), '').otherwise(col(col_name))),dim_date_final.columns,dim_date_final))

#output the files

new_master_dim_date_null_filtered.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').mode('overwrite').save("s3://"+ bucket + "/" + database  + "/" + dimension_working_dir + output_file+"/", compression = 'gzip')

new_hash_dim_date.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').mode('overwrite').save("s3://"+ bucket + "/" + database + "/" + hash_working_dir + output_file +"_HASH/", compression = 'gzip')
