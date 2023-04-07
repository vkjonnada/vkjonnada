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
from pyspark.sql.types import IntegerType

## Initialize pyspark ##
sc = SparkContext().getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## Read parameters from configuration file ##
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

##read data from dcowner source files
tbl_plan = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN/PLAN.csv")
tbl_organization = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ORGANIZATION/ORGANIZATION.csv")
#tbl_financial_ops_admin_code = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "FINANCIAL_OPS_ADMIN_CODE/FINANCIAL_OPS_ADMIN_CODE.csv")
tbl_organization_role = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ORGANIZATION_ROLE/ORGANIZATION_ROLE.csv")
#tbl_agent = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "AGENT/AGENT.csv")
#tbl_country = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "COUNTRY/COUNTRY.csv")
tbl_province = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PROVINCE/PROVINCE.csv")
#tbl_accounting_group_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ACCOUNTING_GROUP_TYPE/ACCOUNTING_GROUP_TYPE.csv")
#tbl_internal_bank_account_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "INTERNAL_BANK_ACCOUNT_TYPE/INTERNAL_BANK_ACCOUNT_TYPE.csv")
tbl_region = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "REGION/REGION.csv")
tbl_irs_code_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "IRS_CODE_TYPE/IRS_CODE_TYPE.csv")
#tbl_plan_state_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_STATE_TYPE/PLAN_STATE_TYPE.csv")
tbl_plan_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_TYPE/PLAN_TYPE.csv")
tbl_plan_agent  = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_AGENT/PLAN_AGENT.csv")
tbl_plan_assoc_code_rlshp  = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_ASSOC_CODE_RLSHP/PLAN_ASSOC_CODE_RLSHP.csv")
tbl_org_role_statement_group_rlshp = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ORG_ROLE_STATEMENT_GROUP_RLSHP/ORG_ROLE_STATEMENT_GROUP_RLSHP.csv")
tbl_agent = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "AGENT/AGENT.csv")
tbl_country = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "COUNTRY/COUNTRY.csv")
tbl_document_group = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "DOCUMENT_GROUP/DOCUMENT_GROUP.csv")
tbl_financial_ops_admin_code = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "FINANCIAL_OPS_ADMIN_CODE/FINANCIAL_OPS_ADMIN_CODE.csv")
tbl_plan_acct_case_code_rlshp = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_ACCT_CASE_CODE_RLSHP/PLAN_ACCT_CASE_CODE_RLSHP.csv")
tbl_statement_group = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "STATEMENT_GROUP/STATEMENT_GROUP.csv")
tbl_plan_auto_increase = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_AUTO_INCREASE/PLAN_AUTO_INCREASE.csv")
tbl_association_code_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ASSOCIATION_CODE_TYPE/ASSOCIATION_CODE_TYPE.csv")
tbl_accounting_case_code_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ACCOUNTING_CASE_CODE_TYPE/ACCOUNTING_CASE_CODE_TYPE.csv")
tbl_accounting_group_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "ACCOUNTING_GROUP_TYPE/ACCOUNTING_GROUP_TYPE.csv")
tbl_internal_bank_account_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "INTERNAL_BANK_ACCOUNT_TYPE/INTERNAL_BANK_ACCOUNT_TYPE.csv")
tbl_report_group_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "REPORT_GROUP_TYPE/REPORT_GROUP_TYPE.csv")
tbl_plan_state_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_STATE_TYPE/PLAN_STATE_TYPE.csv")
tbl_plan_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_TYPE/PLAN_TYPE.csv")
tbl_frequency_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "FREQUENCY_TYPE/FREQUENCY_TYPE.csv")
tbl_plan_sdo_parameter = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_SDO_PARAMETER/PLAN_SDO_PARAMETER.csv")
tbl_plan_service_agreement = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_SERVICE_AGREEMENT/PLAN_SERVICE_AGREEMENT.csv")
tbl_financial_service = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "FINANCIAL_SERVICE/FINANCIAL_SERVICE.csv")
tbl_plan_eligibility  = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_ELIGIBILITY/PLAN_ELIGIBILITY.csv")
tbl_plan_investment_alternative = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_INVESTMENT_ALTERNATIVE/PLAN_INVESTMENT_ALTERNATIVE.csv")
tbl_plan_auto_enrollment = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_AUTO_ENROLLMENT/PLAN_AUTO_ENROLLMENT.csv")
tbl_edelivery_preference_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "EDELIVERY_PREFERENCE_TYPE/EDELIVERY_PREFERENCE_TYPE.csv")
tbl_increase_approach_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "INCREASE_APPROACH_TYPE/INCREASE_APPROACH_TYPE.csv")
tbl_plan_option = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_OPTION")
tbl_plan_option_type = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + "PLAN_OPTION_TYPE")
tbl_investment_option = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'INVESTMENT_OPTION')
tbl_investment_product = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'INVESTMENT_PRODUCT')
tbl_prod_off_inv_prd_rlshp = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PROD_OFF_INV_PRD_RLSHP')
tbl_product_offering_service = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PRODUCT_OFFERING_SERVICE')
tbl_product_offering = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PRODUCT_OFFERING')
tbl_plan_invst_alternative_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PLAN_INVST_ALTERNATIVE_TYPE')
tbl_plan_history = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PLAN_HISTORY')
tbl_plan_trust_arrangement = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PLAN_TRUST_ARRANGEMENT')
tbl_trust_arrangement_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'TRUST_ARRANGEMENT_TYPE')
tbl_plan_trust_arrng_serv_type = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'PLAN_TRUST_ARRNG_SERV_TYPE')
tbl_org_role_rule_restriction = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'ORG_ROLE_RULE_RESTRICTION')
tbl_trust_arrangement = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'TRUST_ARRANGEMENT')

#read the hash file from master directory
tbl_hash = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load("s3://"+ bucket + "/" + database + "/" + hash_dir + output_file + "_HASH")

## Load control proess dates from dcowner file to dataframe. Also load the process dates for application DBATP into variables ##
tbl_cntrl_process_date = spark.read.format("com.databricks.spark.csv").option('delimiter','|').option("header", "true").option("inferSchema", "true").load("s3://"+ bucket + "/" + database + "/" + dcowner_master_dir + 'DM_CONTROL_PROCESS_DATE')
cntrl_process_date= tbl_cntrl_process_date.filter(col("dm_control_app_type_code") == 'DRPTP').selectExpr("to_date(current_process_date) as current_process_date","to_date(previous_process_date) as previous_process_date").collect()
v_current_process_date=cntrl_process_date[0][0]
v_prev_process_date=cntrl_process_date[0][1]

#df_orrr=tbl_org_role_rule_restriction.filter((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date)))

#esop indicator
df_esop_ind = tbl_organization_role.alias("org").join(tbl_org_role_rule_restriction.alias("orrr"),col("org.org_role_seq_id") == col("orrr.org_role_seq_id"),how="left").filter((col("orrr.rule_option_type_code") == 'EMPSOP') & (col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date))).selectExpr("org.org_role_seq_id",
"orrr.rule_option_type_code")
    
#nqdc indicator
df_nqdc_ind = tbl_organization_role.alias("org").join(tbl_org_role_rule_restriction.alias("orrr"),col("org.org_role_seq_id") == col("orrr.org_role_seq_id"),how="left").filter((col("orrr.rule_option_type_code") == 'NQDEXT') & (col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date))).selectExpr("org.org_role_seq_id",
"orrr.rule_option_type_code")

#select only required columns in Organization Role 
df_psorgr=tbl_organization_role.alias("orgr").join(
tbl_edelivery_preference_type.alias("ede"),col("orgr.edelivery_preference_type_code") == col("ede.edelivery_preference_type_code"),how="left").join(
df_esop_ind.alias("esop"),col("orgr.org_role_seq_id") == col("esop.org_role_seq_id"),how="left").join(
df_nqdc_ind.alias("nqdc"),col("orgr.org_role_seq_id") == col("nqdc.org_role_seq_id"),how="left").selectExpr("orgr.org_role_seq_id",
"organization_seq_id",
"plan_sponsor_number",
"ps_vru_code",
"ps_market_message_allowed_ind",
"master_plan_sponsor_seq_id",
"master_plan_sponsor_ind",
"CASE WHEN report_group_type_code IS NULL THEN 'DFLT' ELSE report_group_type_code END as report_group_type_code",
"ede.description as edelivery_prefrence","orgr.ps_web_address_chg_alwd_ind",
"CASE WHEN esop.rule_option_type_code = 'EMPSOP' THEN 'Y' ELSE 'N' END AS esop_ind",
"CASE WHEN nqdc.rule_option_type_code = 'NQDEXT' THEN 'Y' ELSE 'N' END AS nqdc_ind")


#applying rank on plan agent file
df_plan_agent = tbl_plan_agent.alias("pa").withColumn("rank",rank().over(Window.partitionBy([col(x) for x in ["pa.plan_seq_id","pa.agent_seq_id"]]).orderBy(desc("pa.activation_date")))).filter(col("rank")==1).selectExpr(
"pa.plan_seq_id",
"pa.agent_seq_id")

# Get row order by plan seq id and primary indicator 
df_asc_code = tbl_plan_assoc_code_rlshp.alias("pacr").join(tbl_association_code_type.alias("act"),col("pacr.association_code_type_seq_id") == col("act.association_code_type_seq_id"),how="inner").withColumn("row_order",rank().over(Window.partitionBy([col(x) for x in ["pacr.plan_seq_id","act.primary_indicator"]]).orderBy(asc("act.association_code_type_seq_id")))).selectExpr("pacr.plan_seq_id as plan_seq_id",
"act.association_code_type_code as association_code_type_code",
"act.description as description",
"act.primary_indicator as primary_indicator","row_order")


# Setting the column values to Null if certain condition is not matched 
df_asc_code_case_statements = df_asc_code.selectExpr("plan_seq_id","CASE WHEN primary_indicator='Y' THEN association_code_type_code ELSE NULL END as primary_assoc_code",
"CASE WHEN primary_indicator='Y' THEN description ELSE NULL END as primary_assoc_code_description",
"CASE WHEN primary_indicator='N' AND row_order=1 THEN association_code_type_code ELSE NULL END as secondary_assoc_code1",
"CASE WHEN primary_indicator='N' AND row_order=1 THEN description ELSE NULL END as secondary_assoc_description1",
"CASE WHEN primary_indicator='N' AND row_order=2 THEN association_code_type_code ELSE NULL END as secondary_assoc_code2",
"CASE WHEN primary_indicator='N' AND row_order=2 THEN description ELSE NULL END as secondary_assoc_description2")

# get max for Codes with grouping on plan_seq_id 
df_asc_code_max = df_asc_code_case_statements.groupBy("plan_seq_id").agg(max("primary_assoc_code").alias("primary_assoc_code"),max("primary_assoc_code_description").alias("primary_assoc_code_description"),max("secondary_assoc_code1").alias("secondary_assoc_code1"),max("secondary_assoc_description1").alias("secondary_assoc_description1"),max("secondary_assoc_code2").alias("secondary_assoc_code2"),max("secondary_assoc_description2").alias("secondary_assoc_description2"))

# Get row order by plan seq id and primary indicator 
df_pln_act_code =  tbl_plan_acct_case_code_rlshp.alias("paccr").join(tbl_accounting_case_code_type.alias("acct"),col("paccr.acctng_case_code_type_seq_id") == col("acct.acctng_case_code_type_seq_id")).withColumn("row_order",rank().over(Window.partitionBy([col(x) for x in ["paccr.plan_seq_id","acct.primary_indicator"]]).orderBy(desc("acct.acctng_case_code_type_seq_id")))).selectExpr("paccr.plan_seq_id",
"acct.acctng_case_code_type_code",
"acct.description",
"acct.primary_indicator",
"acct.caps_app_number_payroll",
"acct.caps_app_number_loans",
"row_order")

# Setting the column values to Null if certain condition is not matched 
df_pln_act_code_case_statements = df_pln_act_code.selectExpr("plan_seq_id",
"CASE WHEN primary_indicator='Y' THEN acctng_case_code_type_code ELSE NULL END as primary_acctng_case_code",
"CASE WHEN primary_indicator='Y' THEN description ELSE NULL END as primary_acctng_case_desc",
"CASE WHEN primary_indicator='Y' THEN caps_app_number_payroll ELSE NULL END as caps_app_number_payroll",
"CASE WHEN primary_indicator='Y' THEN caps_app_number_loans ELSE NULL END as caps_app_number_loans",
"CASE WHEN primary_indicator='N' AND row_order=1 THEN acctng_case_code_type_code ELSE NULL END as secondary_acctng_case_code",
"CASE WHEN primary_indicator='N' AND row_order=1 THEN description ELSE NULL END as secondary_acctng_case_desc")

# get max for Codes with grouping on plan_seq_id 
df_pln_acc_code_max = df_pln_act_code_case_statements.groupBy("plan_seq_id").agg(max("primary_acctng_case_code").alias("primary_acctng_case_code"), max("primary_acctng_case_desc").alias("primary_acctng_case_desc"), max("caps_app_number_payroll").alias("caps_app_number_payroll"), max("caps_app_number_loans").alias("caps_app_number_loans"), max("secondary_acctng_case_code").alias("secondary_acctng_case_code"), max("secondary_acctng_case_desc").alias("secondary_acctng_case_desc"))

#get max of statement groups
df_stmtgrp_max = tbl_org_role_statement_group_rlshp.alias("orsgr").join(tbl_statement_group.alias("sg"),col("orsgr.statement_group_seq_id")==col("sg.statement_group_seq_id"),how="inner").groupBy("orsgr.plan_spnsr_org_role_seq_id").agg(max("sg.name").alias("stmt_group_name"))


#filter plan auto incerase on current dates 

df_plan_auto_inc = tbl_plan_auto_increase.alias("pai").join(tbl_increase_approach_type.alias("inc"),col("pai.increase_approach_type_code")==col("inc.increase_approach_type_code"),how="left").filter((col("pai.activation_date")<=v_current_process_date) & (col("pai.deactivation_date").isNull() | (col("pai.deactivation_date")>v_current_process_date))).selectExpr("pai.plan_seq_id",
"'Y' as plan_autoincrease_ind",
"pai.frequency_type_code",
"pai.increase_amount","inc.description as increase_approach_type")


#data point for FIA: to get fia enroll date only

df_plan_option = tbl_plan_option.alias("po").join(
tbl_plan_option_type.alias("pot"),col("po.plan_option_type_seq_id")==col("pot.plan_option_type_seq_id")).filter((col("plan_option_type_code") == 'TRMPRO' ) & ((col("po.deactivation_date").isNull()) | (col("po.deactivation_date")>v_current_process_date))).selectExpr("plan_seq_id","activation_date as fia_enroll_date")

#Add ESOP and NQDC data points
df_p_hist=tbl_plan_history.filter((col("new_value") == 'ACTIVE') & (col("plan_history_type_code") == 'STATE')).groupBy("plan_seq_id").agg(max("change_date").alias("plan_activation_date")).selectExpr("plan_seq_id","date_format(plan_activation_date,'yyyy-MM-dd') as plan_activation_date")

df_trust_arrangement=tbl_trust_arrangement.filter((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date)))

df_plan_trust_arrangement_act=tbl_plan_trust_arrangement.filter((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date)))

#Applying rank to get the latest plan_trust_arrangement for a plan
df_plan_trust_arrangement = df_plan_trust_arrangement_act.alias("pta").withColumn("rank",rank().over(Window.partitionBy([col(x) for x in ["pta.plan_seq_id"]]).orderBy(desc("pta.plan_trust_arrangement_seq_id")))).filter(col("rank")==1).selectExpr(
"pta.plan_trust_arrangement_seq_id","pta.plan_seq_id", "pta.activation_date", "pta.deactivation_date", "pta.trust_arrangement_seq_id", "pta.plan_trust_serv_type_code", "pta.allow_ntc_cash_process_fee", "pta.effective_date", "pta.last_updated_by")

df_trust=df_trust_arrangement.alias("ta").join(tbl_trust_arrangement_type.alias("tat"),col("ta.trust_arrangement_type_code")==col("tat.trust_arrangement_type_code"),how="left").join(tbl_organization_role.alias("t_orgr"),col("ta.trust_company_org_role_seq_id")==col("t_orgr.org_role_seq_id")).selectExpr("ta.trust_arrangement_seq_id",
"ta.trust_arrangement_type_code", "ta.trust_company_org_role_seq_id","ta.pehp_trustee_org_role_seq_id",
"tat.trust_arrangement_type_code as tat_trust_arrangement_type_code","tat.description","t_orgr.org_role_seq_id","t_orgr.organization_seq_id")

df_trustee=df_trust_arrangement.alias("ta").join(tbl_trust_arrangement_type.alias("tat"),col("ta.trust_arrangement_type_code")==col("tat.trust_arrangement_type_code"),how="left").join(tbl_organization_role.alias("t_orgr"),col("ta.pehp_trustee_org_role_seq_id")==col("t_orgr.org_role_seq_id")).selectExpr("ta.trust_arrangement_seq_id",
"ta.trust_arrangement_type_code", "ta.trust_company_org_role_seq_id","ta.pehp_trustee_org_role_seq_id",
"tat.trust_arrangement_type_code as tat_trust_arrangement_type_code","tat.description","t_orgr.org_role_seq_id","t_orgr.organization_seq_id")

df_trust_trustee=df_trust.union(df_trustee).distinct()

df_esop=tbl_plan.alias("p").join(
df_plan_trust_arrangement.alias("pta"),col("p.plan_seq_id")==col("pta.plan_seq_id"),how="left").join(
df_trust_trustee.alias("tt"),col("pta.trust_arrangement_seq_id")== col("tt.trust_arrangement_seq_id"),how="left").join(
tbl_plan_trust_arrng_serv_type.alias("ptast"),col("pta.plan_trust_serv_type_code")==col("ptast.plan_trust_serv_type_code"),how="left").join(
tbl_organization.alias("t_org"),col("tt.organization_seq_id")==col("t_org.organization_seq_id"),how="left").join(
df_p_hist.alias("p_hist"),col("p.plan_seq_id")==col("p_hist.plan_seq_id"),how="left").selectExpr("p.plan_seq_id",
"p.erisa_plan_ind",	
"p_hist.plan_activation_date",
"date_format(pta.activation_date,'yyyy-MM-dd') as trust_effective_date",
"date_format(pta.deactivation_date,'yyyy-MM-dd') as trust_termination_date",
"NVL(tt.trust_arrangement_type_code, 'N/A') as trust_arrangement_type_code",
"NVL(tt.description,'Not Applicable') as trust_arrangement_type",
"NVL(pta.plan_trust_serv_type_code, 'N/A') as plan_trust_serv_type_code",
"NVL(ptast.description,'Not Applicable') as plan_trust_serv_type",
"t_org.name	AS provider",
"pta.allow_ntc_cash_process_fee") 

dim_plan_stg = tbl_plan.alias("p").join(
df_psorgr.alias("psorgr"),col("p.plan_spnsr_org_role_seq_id")==col("psorgr.org_role_seq_id")).join(
tbl_organization.alias("psorg"),col("psorgr.organization_seq_id")==col("psorg.organization_seq_id")).join(
tbl_financial_ops_admin_code.alias("foac"),col("p.fin_ops_admin_code_seq_id")==col("foac.fin_ops_admin_code_seq_id")).join(
tbl_organization_role.alias("irkorgr"),col("foac.irk_org_role_seq_id")==col("irkorgr.org_role_seq_id")).join(tbl_organization.alias("irkorg"),col("irkorgr.organization_seq_id")==col("irkorg.organization_seq_id")).join(
tbl_document_group.alias("dg"),col("p.document_group_seq_id")==col("dg.document_group_seq_id")).join(df_plan_agent.alias("pa"),col("p.plan_seq_id")==col("pa.plan_seq_id")).join(
tbl_agent.alias("ag"),col("pa.agent_seq_id")==col("ag.agent_seq_id")).join(
tbl_organization_role.alias("agncyorgr"),col("ag.agency_org_role_seq_id")==col("agncyorgr.org_role_seq_id")).join(
tbl_organization.alias("agncyorg"),col("agncyorgr.organization_seq_id")==col("agncyorg.organization_seq_id")).join(
tbl_country.alias("cntry"),col("p.country_seq_id")==col("cntry.country_seq_id")).join(
tbl_province.alias("prvnc"),col("p.province_seq_id")==col("prvnc.province_seq_id")).join(
tbl_accounting_group_type.alias("agt"),col("p.acctng_group_type_seq_id")==col("agt.acctng_group_type_seq_id")).join(
tbl_internal_bank_account_type.alias("ibat"),col("p.internal_bank_account_seq_id")==col("ibat.internal_bank_account_seq_id")).join(
tbl_region.alias("r"),col("p.region_seq_id")==col("r.region_seq_id")).join(
tbl_irs_code_type.alias("ict"),col("p.irs_code")==col("ict.irs_code")).join(
tbl_plan_state_type.alias("pst"),col("p.plan_state_type_code")==col("pst.plan_state_type_code")).join(
tbl_plan_type.alias("pt"),col("p.plan_type_code")==col("pt.plan_type_code")).join(
df_asc_code_max.alias("ascode"),col("p.plan_seq_id")==col("ascode.plan_seq_id")).join(
df_pln_acc_code_max.alias("accode"),col("p.plan_seq_id")==col("accode.plan_seq_id")).join(
df_stmtgrp_max.alias("stmtgrp"),col("p.plan_spnsr_org_role_seq_id")==col("stmtgrp.plan_spnsr_org_role_seq_id"),how="left").join(
tbl_report_group_type.alias("rgt"),col("psorgr.report_group_type_code")==col("rgt.report_group_type_code")).join(
tbl_frequency_type.alias("facft"),col("p.facs_report_freq_type_code")==col("facft.frequency_type_code"),how="left").join(
tbl_frequency_type.alias("sraft"),col("p.suraudit_report_freq_type_code")==col("sraft.frequency_type_code"),how="left").join(
df_plan_auto_inc.alias("plan_auto_inc"),col("p.plan_seq_id")==col("plan_auto_inc.plan_seq_id"),how="left").join(
df_plan_option.alias("po"),col("p.plan_seq_id")==col("po.plan_seq_id"),how="left").join(
df_esop.alias("esop"),col("p.plan_seq_id")==col("esop.plan_seq_id"),how="left").selectExpr("p.plan_seq_id as plan_seq_id"
,"p.legal_name as plan_legal_name"
,"p.irs_code as irs_code"
,"ict.description as irs_code_desc"
,"p.plan_state_type_code as plan_status_type_code"
,"pst.description as plan_status_type"
,"foac.code as finops_admin_type_code"
,"foac.description as finops_admin_code_desc"
,"agt.acctng_group_type_code as accounting_group_type_code"
,"agt.description as accounting_group_type"
,"lpad(cast(ibat.internal_bank_acct_type_code as string) ,2,'0') as internal_bank_acct_type_code"
,"ibat.description as internal_bank_acct_type"
,"p.plan_type_code as plan_type_code"
,"pt.description as plan_type"
,"rgt.report_group_type_code as report_group_type_code"
,"rgt.description as report_group_type"
,"p.facs_report_freq_type_code as facs_report_freq_type_code"
,"facft.description as facs_report_freq_type"
,"p.suraudit_report_freq_type_code as suraudit_report_freq_type_code"
,"sraft.description as suraudit_report_freq_type"
,"ascode.primary_assoc_code as primary_assoc_code"
,"ascode.primary_assoc_code_description as primary_assoc_code_desc"
,"ascode.secondary_assoc_code1 as secondary_assoc_code1"
,"ascode.secondary_assoc_description1 as secondary_assoc_code_desc1"
,"ascode.secondary_assoc_code2 as secondary_assoc_code2"
,"ascode.secondary_assoc_description2 as secondary_assoc_code_desc2"
,"accode.primary_acctng_case_code as primary_acctng_case_code"
,"accode.primary_acctng_case_desc as primary_acctng_case_desc"
,"accode.secondary_acctng_case_code as secondary_acctng_case_code"
,"accode.secondary_acctng_case_desc as secondary_acctng_case_desc"
,"p.plan_repting_acctng_case_code as plan_repting_acctng_case_code"
,"p.plan_repting_acctng_group as plan_repting_acctng_group"
,"p.region_seq_id as region_seq_id"
,"r.code as region_code"
,"r.description as region_desc"
,"ict.mixed_case_description as irs_display_name"
,"p.plan_vru_code as plan_vru_code"
,"psorg.organization_seq_id as plan_sponsor_org_seq_id"
,"psorg.name as plan_sponsor_org_name"
,"lpad(cast(psorgr.plan_sponsor_number as string) ,7,'0') as plan_sponsor_number"
,"lpad(cast(p.plan_number as string) ,3,'0') as plan_number"
,"p.plan_spnsr_org_role_seq_id as plan_spnsr_org_role_seq_id"
,"irkorg.organization_seq_id as irk_org_seq_id"
,"irkorg.name as irk_org_name"
,"p.document_group_seq_id as document_group_seq_id"
,"dg.name as document_group_name"
,"ag.agent_id as servicing_agent_id"
,"agncyorg.organization_seq_id as agency_org_seq_id"
,"agncyorg.name as servicing_agency_org_name"
,"cntry.name as country_name"
,"cntry.iso_code as country_iso_code"
,"prvnc.abbreviation as province_abbreviation"
,"prvnc.name as province_name"
,"p.original_effective_date as original_effective_date"
,"p.conversion_date as conversion_date"
,"p.prior_rcrdkpr as prior_rcrdkpr"
,"p.prior_rcrdkpng_id as prior_rcrdkpng_id"
,"p.plan_hold as plan_hold"
,"date_trunc('DD',p.created_date) as dcd_plan_created_date"
,"p.loan_alwd_ind as loan_alwd_ind"
,"p.gasb_print_letter_ind as gasb_print_letter_ind"
,"p.facs_report_receive_ind as facs_report_receive_ind"
,"p.minimum_annual_contrb_amount as minimum_annual_contrb_amount"
,"p.gasb_display_last_4qtr_ind as gasb_display_last_4qtr_ind"
,"p.suraudit_report_receive_ind as suraudit_report_receive_ind"
,"p.bank_group_number as bank_group_number"
,"p.fiscal_year_start_month as fiscal_year_start_month"
,"p.fiscal_year_end_month as fiscal_year_end_month"
,"p.multi_employer_alwd_ind as multi_employer_alwd_ind"
,"psorgr.ps_vru_code as ps_vru_code"
,"psorgr.ps_market_message_allowed_ind as ps_market_message_allowed_ind"
,"accode.caps_app_number_payroll as caps_app_number_payroll"
,"accode.caps_app_number_loans as caps_app_number_loans"
,"NVL (stmtgrp.stmt_group_name, 'N/A') as statement_group_name"
,"p.super_plan_seq_id as super_plan_seq_id"
,"p.super_plan_indicator as super_plan_indicator"
,"psorgr.master_plan_sponsor_seq_id as master_plan_sponsor_seq_id"
,"psorgr.master_plan_sponsor_ind as master_plan_sponsor_ind"
,"NVL (plan_auto_inc.plan_autoincrease_ind, 'N') as plan_autoincrease_ind"
,"plan_auto_inc.increase_amount as plan_auto_increase_amount"
,"plan_auto_inc.frequency_type_code as auto_increase_freq_type_code",
"p.beneficiaries_req_ind","p.web_online_enrlmnt_alwd_ind","p.rehired_annuitant_alwd_ind",
"psorgr.edelivery_prefrence","plan_auto_inc.increase_approach_type","psorgr.ps_web_address_chg_alwd_ind",
"po.fia_enroll_date as fia_enroll_date",
"esop.erisa_plan_ind",	
"esop.plan_activation_date",
"esop.trust_effective_date",
"esop.trust_termination_date",
"esop.trust_arrangement_type_code",
"esop.trust_arrangement_type",
"esop.plan_trust_serv_type_code",
"esop.plan_trust_serv_type",
"esop.provider",
"esop.allow_ntc_cash_process_fee",
"psorgr.esop_ind",
"psorgr.nqdc_ind")

#adding new features

df_sdo_ind = tbl_plan_sdo_parameter.filter((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date))).selectExpr("plan_seq_id").distinct()
 
df_pro_account_ind = tbl_plan_service_agreement.alias("psa").join(
tbl_financial_service.alias("fs"),col("psa.financial_service_seq_id") == col("fs.financial_service_seq_id")).filter((col("fs.name") == 'Pro Account I') & ((col("psa.activation_date")<=v_current_process_date) & (col("psa.deactivation_date").isNull() | (col("psa.deactivation_date")>v_current_process_date)))).selectExpr("psa.plan_seq_id")
 
df_default_invst_alternative = tbl_plan_investment_alternative.filter((col("class_identifier") == 'DefaultInvestmentAlternative') &((col("plan_invst_altrntv_type_code") == 'OTHER') | (col("plan_invst_altrntv_type_code") == 'TARMAT')) & ((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date)))).selectExpr("plan_seq_id").distinct()

df_plan_eligibility = tbl_plan_eligibility.filter(col("eligibility_status_type_code") == 'ACTIVE')

df_plan_auto_enrollment = tbl_plan_auto_enrollment.filter((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date))).selectExpr("plan_seq_id","plan_auto_enrl_partc_ind").distinct()
 
 
### Start of Changes for Income America project ###

df_ia_investment_option=tbl_investment_option.alias("io").filter((col("io.activation_date")<=v_current_process_date) & ((col("io.deactivation_date").isNull()) | (col("io.deactivation_date")>v_current_process_date)))

df_ia_investment_product=tbl_investment_product.alias("ip").filter((col("ip.activation_date")<=v_current_process_date) & ((col("ip.deactivation_date").isNull()) | (col("ip.deactivation_date")>v_current_process_date)))

df_ia_prod_off_inv_prd_rlshp=tbl_prod_off_inv_prd_rlshp.alias("poipr").filter((col("poipr.activation_date")<=v_current_process_date) & ((col("poipr.deactivation_date").isNull()) | (col("poipr.deactivation_date")>v_current_process_date)))

df_ia_product_offering_service=tbl_product_offering_service.alias("pos").filter((col("pos.activation_date")<=v_current_process_date) & ((col("pos.deactivation_date").isNull()) | (col("pos.deactivation_date")>v_current_process_date)))

df_ia_product_offering=tbl_product_offering.alias("po").filter((col("po.activation_date")<=v_current_process_date) & (col("po.prod_offering_legal_name")=='Income America') & ((col("po.deactivation_date").isNull()) | (col("po.deactivation_date")>v_current_process_date)))

df_financial_service=tbl_financial_service.alias("fs").filter(col("fs.name")=='Income America')

df_income_america_ind=df_ia_investment_option.alias("io").join(df_ia_investment_product.alias("ip"),col("ip.investment_product_seq_id")==col("io.investment_product_seq_id"),how="inner").join(df_ia_prod_off_inv_prd_rlshp.alias("poipr"),col("ip.investment_product_seq_id")==col("poipr.investment_product_seq_id"),how="inner").join(df_ia_product_offering_service.alias("pos"),col("poipr.product_offering_seq_id")==col("pos.product_offering_seq_id"),how="inner").join(df_ia_product_offering.alias("po"),col("pos.product_offering_seq_id")==col("po.product_offering_seq_id"),how="inner").join(df_financial_service,col("pos.financial_service_seq_id")==col("fs.financial_service_seq_id"),how="inner").selectExpr("io.plan_seq_id")

### End of Changes for Income America project ### 

#LIB DataPoints

df_invest_TypeCode = tbl_plan_investment_alternative.alias("pia").join(
tbl_plan_invst_alternative_type.alias("piat"),col("pia.plan_invst_altrntv_type_code") == col("piat.plan_invst_altrntv_type_code"),how="left").filter((col("class_identifier") == 'DefaultInvestmentAlternative') & ((col("activation_date")<=v_current_process_date) & (col("deactivation_date").isNull() | (col("deactivation_date")>v_current_process_date)))).selectExpr("pia.plan_seq_id","pia.plan_invst_altrntv_type_code","pia.qualified_ind","piat.description as plan_invst_altrntv_type").distinct()

### Added for LIB project ###
df_lib_ind = tbl_plan_service_agreement.alias("l").join(
tbl_financial_service.alias("fs"),col("l.financial_service_seq_id") == col("fs.financial_service_seq_id")).filter((col("fs.name") == 'Lifetime Income Builder') & ((col("l.activation_date")<=v_current_process_date) & (col("l.deactivation_date").isNull() | (col("l.deactivation_date")>v_current_process_date)))).selectExpr("l.plan_seq_id").distinct()
 

#joining to dim_plan_stg for newly added cols 
 
dim_plan_final_stg = dim_plan_stg.alias("pl").join(
df_sdo_ind.alias("sdo"),col("pl.plan_seq_id")==col("sdo.plan_seq_id"),how = "left").join(
df_pro_account_ind.alias("pro"),col("pl.plan_seq_id")==col("pro.plan_seq_id"),how = "left").join(
df_default_invst_alternative.alias("dia"),col("pl.plan_seq_id")==col("dia.plan_seq_id"),how = "left").join(
df_plan_auto_enrollment.alias("pae"),col("pl.plan_seq_id")==col("pae.plan_seq_id"),how = "left").join(
df_plan_eligibility.alias("pe"),col("pl.plan_seq_id")==col("pe.plan_seq_id"),how = "left").join(
df_income_america_ind.alias("ia"),col("pl.plan_seq_id")==col("ia.plan_seq_id"),how="left").join(
df_invest_TypeCode.alias("itc"),col("pl.plan_seq_id")==col("itc.plan_seq_id"),how="left").join(
df_lib_ind.alias("lib"),col("pl.plan_seq_id")==col("lib.plan_seq_id"),how = "left").selectExpr("pl.*","NVL2(sdo.plan_seq_id,'Y','N') as sdo_ind",
"NVL2(pro.plan_seq_id,'Y','N') as pro_acc_ind",
"NVL2(dia.plan_seq_id,'Y','N') as default_invst_alt_ind",
"pae.plan_auto_enrl_partc_ind",
"NVL2(pe.plan_seq_id,'Y','N') as elgb_mgmnt_srvc_ind",
"CASE WHEN ia.plan_seq_id IS NULL THEN 'N' ELSE 'Y' END as income_america_indicator",
"NVL (itc.plan_invst_altrntv_type_code, 'N/A') as plan_invst_altrntv_type_code",
"NVL(itc.qualified_ind,'N') as qualified_ind",
"NVL (itc.plan_invst_altrntv_type,'Not Applicable') as plan_invst_altrntv_type",
"NVL2(lib.plan_seq_id,'Y','N') as lifetime_income_builder_ind").distinct()


### Generating New Hash file and Target file
#get the highest key_val
max_plan_key=tbl_hash.agg(max(col("PLAN_KEY"))).collect()[0][0]

#calculate hash value for the data in staging file
df_plan_stg_hash_cal=dim_plan_final_stg.withColumn("HASH_VALUE",sha2(concat_ws("||",*hash_columns_list),512))

#join staging data frame  with existing old hash file to get the Keys for existing records and generate new keys for new inserts

new_master_dim_plan =df_plan_stg_hash_cal.alias("stg").join(tbl_hash.alias("h").filter(col("h.plan_seq_id").isNotNull()),col("stg.plan_seq_id")==col("h.plan_seq_id") ,how='left').selectExpr("CASE WHEN h.plan_seq_id IS NULL THEN row_number() over (order by h.plan_seq_id)+"+ str(max_plan_key)+" ELSE h.PLAN_KEY END as PLAN_KEY","stg.*","NVL(h.create_date,current_timestamp()) as CREATE_DATE","CASE WHEN (h.plan_seq_id IS NULL OR (h.HASH_VALUE<>stg.HASH_VALUE)) THEN current_timestamp() ELSE h.LAST_UPDATE_DATE END as LAST_UPDATE_DATE")

# To make the keys as persistant values

new_master_dim_plan.persist()

#reordering columns and setting default values as per DDL

dim_plan_final = new_master_dim_plan.selectExpr("plan_key",
"plan_seq_id",
"plan_legal_name",
"irs_code",
"irs_code_desc",
"plan_status_type_code",
"plan_status_type",
"finops_admin_type_code",
"finops_admin_code_desc",
"accounting_group_type_code",
"accounting_group_type",
"internal_bank_acct_type_code",
"internal_bank_acct_type",
"plan_type_code",
"plan_type",
"report_group_type_code",
"report_group_type",
"facs_report_freq_type_code",
"facs_report_freq_type",
"suraudit_report_freq_type_code",
"suraudit_report_freq_type",
"primary_assoc_code",
"primary_assoc_code_desc",
"secondary_assoc_code1",
"secondary_assoc_code_desc1",
"secondary_assoc_code2",
"secondary_assoc_code_desc2",
"primary_acctng_case_code",
"primary_acctng_case_desc",
"secondary_acctng_case_code",
"secondary_acctng_case_desc",
"plan_repting_acctng_case_code",
"plan_repting_acctng_group",
"region_seq_id",
"region_code",
"region_desc",
"irs_display_name",
"plan_vru_code",
"plan_sponsor_org_seq_id",
"plan_sponsor_org_name",
"plan_sponsor_number",
"plan_number",
"plan_spnsr_org_role_seq_id",
"irk_org_seq_id",
"irk_org_name",
"document_group_seq_id",
"document_group_name",
"lpad(cast(servicing_agent_id as string) ,5,'0') as servicing_agent_id",
"agency_org_seq_id",
"servicing_agency_org_name",
"country_name",
"country_iso_code",
"province_abbreviation",
"province_name",
"original_effective_date",
"conversion_date",
"prior_rcrdkpr",
"prior_rcrdkpng_id",
"plan_hold",
"dcd_plan_created_date",
"loan_alwd_ind",
"gasb_print_letter_ind",
"facs_report_receive_ind",
"minimum_annual_contrb_amount",
"gasb_display_last_4qtr_ind",
"suraudit_report_receive_ind",
"bank_group_number",
"fiscal_year_start_month",
"fiscal_year_end_month",
"multi_employer_alwd_ind",
"ps_vru_code",
"ps_market_message_allowed_ind",
"caps_app_number_payroll",
"caps_app_number_loans",
"statement_group_name",
"create_date",
"last_update_date",
"super_plan_seq_id",
"super_plan_indicator",
"master_plan_sponsor_seq_id",
"master_plan_sponsor_ind",
"plan_auto_increase_amount",
"auto_increase_freq_type_code",
"plan_autoincrease_ind",
"sdo_ind",
"pro_acc_ind",
"default_invst_alt_ind",
"plan_auto_enrl_partc_ind",
"elgb_mgmnt_srvc_ind",
"beneficiaries_req_ind",
"web_online_enrlmnt_alwd_ind",
"rehired_annuitant_alwd_ind",
"edelivery_prefrence",
"increase_approach_type","ps_web_address_chg_alwd_ind","date_format(fia_enroll_date,'yyyy-MM-dd') as fia_enroll_date","income_america_indicator",
"plan_invst_altrntv_type_code","plan_invst_altrntv_type","qualified_ind","lifetime_income_builder_ind",
"erisa_plan_ind",	
"plan_activation_date",
"trust_effective_date",
"trust_termination_date",
"trust_arrangement_type_code",
"trust_arrangement_type",
"plan_trust_serv_type_code",
"plan_trust_serv_type",
"provider",
"allow_ntc_cash_process_fee",
"esop_ind",
"nqdc_ind")


#generate new hash file with old keysand new keys and updated hash value an it doesnot contain history
new_hash_dim_plan=new_master_dim_plan.selectExpr("PLAN_KEY","PLAN_SEQ_ID","CREATE_DATE","LAST_UPDATE_DATE","HASH_VALUE").orderBy(["PLAN_KEY"],ascending=[1]).distinct()																																				  

#Get the records from old hash file that are dropped in new hash file (new_hash_dim_plan)
history_hash = tbl_hash.alias("h").join(new_hash_dim_plan.alias("new"),col("h.PLAN_KEY") == col("new.PLAN_KEY"),how = "left").filter(col("new.PLAN_KEY").isNull()).selectExpr("h.PLAN_KEY","h.PLAN_SEQ_ID","h.CREATE_DATE","h.LAST_UPDATE_DATE","h.HASH_VALUE")
																																	  

#final hash file contains new,changed,no change and deleted records
final_full_hash_file = history_hash.union(new_hash_dim_plan).distinct()

#output the files

new_master_dim_plan_null_filtered = (functools.reduce(lambda result, col_name: result.withColumn(col_name, when(col(col_name).isNull(), '').otherwise(col(col_name))),dim_plan_final.columns,dim_plan_final))

new_master_dim_plan_null_filtered.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').mode('overwrite').save("s3://"+ bucket + "/" + database  + "/" + dimension_working_dir + output_file+"/", compression = 'gzip')

final_full_hash_file.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", '|').mode('overwrite').save("s3://"+ bucket + "/" + database + "/" + hash_working_dir + output_file +"_HASH/", compression = 'gzip')
