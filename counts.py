# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

# %%
path = r"/home/klaus/Repos/ds/tech_job_postings/scrape/parsed_postings.parquet"

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
# df.printSchema()
df = df.withColumn("listing_date", df["listing_date"].cast("date"))
# df.printSchema()

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

df_job_titles = df.select("link", "job_title")

# regex patterns
job_title_list = [
    "data",
    "data engineer",
    "data architect",
    "data scientist",
    "data analyst",
    r"(ml|machine learning)",
    r"(ai|artificial intelligence)",
    # "engineer",
]
for jt in job_title_list:
    df_job_titles = df_job_titles.withColumn(jt, F.regexp_count("job_title", F.lit(jt)))

titles_with_data = [F.col(jt) for jt in job_title_list if "data" in jt]
data_with_other_word = titles_with_data[1:]
df_job_titles = df_job_titles.withColumn(
    "data_combined_row_sum", sum(data_with_other_word)
)
df_job_titles = df_job_titles.withColumn(
    "only_data_found",
    ((df_job_titles["data"] >= 1) & (df_job_titles["data_combined_row_sum"] == 0)),
)

# only data (sus?)
df_job_titles.select("*").where("only_data_found").toPandas()

# most frequent
df_job_titles.groupby().sum().toPandas()

# %%
# word counts
df_title_word_counts = df.select("link", "job_title")

drop_parentheses = True
drop_slash = True
drop_hyphen = True


parentheses_pattern = r"(\(|\))"
if drop_parentheses:
    df_title_word_counts = df_title_word_counts.withColumn(
        "job_title", F.regexp_replace("job_title", parentheses_pattern, r"")
    )
if drop_slash:
    df_title_word_counts = df_title_word_counts.withColumn(
        "job_title", F.regexp_replace("job_title", r"/", r" ")
    )
if drop_hyphen:
    df_title_word_counts = df_title_word_counts.withColumn(
        "job_title", F.regexp_replace("job_title", r"-", r" ")
    )

df_title_word_counts = df_title_word_counts.withColumn(
    "words", F.split(F.col("job_title"), " ")
)

df_title_word_counts = df_title_word_counts.select(F.explode("words"))
df_title_word_counts.toPandas()
df_title_word_counts.groupby("col").count().orderBy("count", ascending=False).toPandas()

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
