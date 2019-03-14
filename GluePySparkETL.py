import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#Add:
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "teachersmg", table_name = "deduplicate", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "teachersmg", table_name = "deduplicate", transformation_ctx = "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("col0", "long", "id", "long"), ("col1", "string", "name", "string"), ("col2", "string", "descsitser", "string"), ("col3", "string", "job_title", "string"), ("col4", "string", "retirement", "string"), ("col5", "string", "commission_job_title", "string"), ("col6", "string", "institution", "string"), ("col7", "string", "school", "string"), ("col8", "double", "hours", "double"), ("col9", "double", "payment", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col0", "long", "id", "long"), ("col1", "string", "name", "string"), ("col3", "string", "job_title", "string"), ("col4", "string", "retirement", "string"), ("col5", "string", "commission_job_title", "string"), ("col6", "string", "institution", "string"), ("col7", "string", "school", "string"), ("col8", "double", "hours", "double"), ("col9", "double", "payment", "double")], transformation_ctx = "applymapping1")



# To Spark DF
df = applymapping1.toDF()

# name
df = df.withColumn("name_2", regexp_replace( col("name"), "A", "@" ))
df = df.withColumn("name_3", regexp_replace( col("name_2"), "E", "_" ))
df = df.withColumn("name_4", regexp_replace( col("name_3"), "I", "~" )) 
df = df.withColumn("name_5", regexp_replace( col("name_4"), "O", "*" ))
df = df.withColumn("name_6", regexp_replace( col("name_5"), "U", "%" ))
df = df.drop('name')
df = df.drop('name_2')
df = df.drop('name_3')
df = df.drop('name_4')
df = df.drop('name_5')
df = df.withColumnRenamed("name_6", "name")
print("[LOG]","name transformed")

# retirement
df = df.withColumn('retirement_2', when( col("retirement")=='Nao',0).otherwise(1) )
df = df.drop('retirement')
df = df.withColumnRenamed("retirement_2", "retirement")
print("[LOG]","retirement transformed")

# payment ( to be the last )
df = df.withColumn('payment_2', col("payment") )
df = df.drop('payment')
df = df.withColumnRenamed("payment_2", "payment")
print("[LOG]","payment transformed")



# to Glue applymapping2
applymapping2 = DynamicFrame.fromDF(df, glueContext, "applymapping2")

print("[LOG]","fields transformed")


## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://alfjnr-datasets/dataset-teachersMG/teachers_transformed"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping2, connection_type = "s3", connection_options = {"path": "s3://alfjnr-datasets/dataset-teachersMG/teachers_transformed"}, format = "csv", transformation_ctx = "datasink2")
job.commit()