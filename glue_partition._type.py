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

# Mapping the correct data types
apply_mapping = ApplyMapping.apply(frame=datasource0,
    mappings=[
        ("prefecture_code", "string", "prefecture_code", "string"),
        ("prefecture_name", "string", "prefecture_name", "string"),
        ("era", "string", "era", "string"),
        ("japanese_calendar_year", "bigint", "japanese_calendar_year", "bigint"),
        ("western_calendar_year", "bigint", "western_calendar_year", "bigint"),
        ("notes", "string", "notes", "string"),
        ("total_population", "bigint", "total_population", "bigint"),
        ("male_population", "bigint", "male_population", "bigint"),
        ("female_population", "bigint", "female_population", "bigint")
    ])

# Write data to S3 bucket, partitioned by western_calendar_year
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=apply_mapping,  # Note the change here from datasource0 to apply_mapping
    connection_type="s3",
    connection_options={
        "path": "s3://practice-glue-crawler/population_by_prefecture_year_partitioned/",
        "partitionKeys": ["western_calendar_year"]
    },
    format="parquet"
)

job.commit()
