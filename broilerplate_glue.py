import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read data from the Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="<your_database_name>",
    table_name="<your_table_name>"
)

# Convert the dynamic frame to a Spark DataFrame
data_frame = dynamic_frame.toDF()

# Extract the required columns from the DataFrame and load into Redshift
required_columns = ['column1', 'column2', ...]  # Add your column names here
data_frame.select(required_columns).write \
    .format("jdbc") \
    .option("url", "<redshift_jdbc_url>") \
    .option("dbtable", "<redshift_table_name>") \
    .option("user", "<redshift_username>") \
    .option("password", "<redshift_password>") \
    .mode("append") \
    .save()

job.commit()
