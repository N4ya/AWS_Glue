import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from the Glue Data Catalog table
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="practice_crawler_zenn", table_name="zenn_input")

# Write data to S3 bucket, partitioned by western_calendar_year
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=datasource0,
    connection_type="s3",
    connection_options={
        "path": "s3://practice-glue-crawler/output/",
        "partitionKeys": ["western_calendar_year"]
    },
    format="parquet"
)

job.commit()