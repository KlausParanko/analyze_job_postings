# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

# %%
path = r"/home/klaus/Repos/ds/job_postings_scrape/parsed_postings.parquet"

spark = SparkSession.builder.getOrCreate()

# %%
technologies = [
    "azure synapse",
    "docker",
    "pytorch",
    "scikit",
    "tableau",
    "power bi",
    "matplotlib",
    "parquet",
    "rabbitmq",
    "kubernetes",
    "dbt",
    "snowflake",
    "databricks",
    "aws",
    "azure",
    "kafka",
    "spark",
    "airflow",
    "luigi",
    "hadoop",
    "cassandra",
    "gcp",
    "sql",
    "python",
    "mysql",
    "postgresql",
    "mongodb",
    "cassandra",
    "synapse",
    "teradata",
    "datafactory",
    "datavault",
    "google cloud",
    "qlik",
    "redshift",
    "big query",
    "glue",
    "terraform",
    "etl",
    "elt",
    "warehous",
    "lakehous",
    "kubernetes",
    "ci/cd",
    "graphql",
    "dask",
    "sagemaker",
    "data explorer",
    "infrastructure as code",
    "iac",
    "streaming",
    "looker",
    "github",
]

# %%
# READ IN DATA
df = spark.read.parquet(path)
df.printSchema()
df = df.withColumn("listing_date", df["listing_date"].cast("date"))
df.printSchema()

df.show(vertical=True)
df.select("link").show()

# %%
# links are unique, use them as id
df.select("link").distinct().count() == df.count()

# WRANGLE
# lowercase all elements
for c in df.columns:
    df = df.withColumn(c, F.lower(c))

# Some postings use eg. power bi while others powerbi. Removing spaces standardizes this.
df = df.withColumn("raw_text", F.regexp_replace("raw_text", " ", ""))
tech_with_no_spaces = [x.replace(" ", "") for x in technologies]


# LOOK AT JOB TITLES
job_title_list = [
    "data",
    "data engineer",
    "data architect",
    "data scientist",
    "data analyst",
    "ml",
    "machine learning",
    "ai",
    "artificial intelligence",
]
df_job_titles = df.select("link", "job_title")
for jt in job_title_list:
    df_job_titles = df_job_titles.withColumn(jt, F.regexp("job_title", F.lit(jt)))


for jt in job_title_list:
    df_job_titles = df_job_titles.withColumn(
        jt, F.when(F.regexp(df_job_titles["job_title"], F.lit(jt)), 1).otherwise(0)
    )


df_job_titles.where(df_job_titles["data engineer"] == F.lit(True)).count()


# %%
# LOOK AT RAW TEXT
counts = df.select("link", "raw_text")
for search_word in tech_with_no_spaces:
    counts = counts.withColumn(
        search_word,
        F.regexp_count("raw_text", F.lit(search_word)),
    )

# non-distinct sum
non_distinct_sum = counts.groupby().sum()
non_distinct_sum.toPandas()
# only count max one occurrence per posting
distinct_sum = counts.drop("link", "raw_text")
for c in distinct_sum.columns:
    distinct_sum = distinct_sum.withColumn(c, F.when(F.col(c) == 0, 0).otherwise(1))
distinct_sum = distinct_sum.groupby().sum()


# %%
# max amount of tech in single posting
max_counts = counts.drop("link", "raw_text")
max_counts = max_counts.groupby().max()
# visualize
max_counts.toPandas().T.sort_values(0, ascending=False)
# order
