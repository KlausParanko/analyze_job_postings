# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd

# %%
path = r"/home/klaus/Repos/ds/tech_job_postings/scrape/data/parsed_postings/parsed_postings.parquet"

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

# regex patterns
job_title_list = [
    "data",
    "data engineer",
    "data architect",
    "data scientist",
    "data analyst",
    "analyst",
    r"(ml|machine learning)",
    r"(ai|artificial intelligence)",
    # "engineer",
]


def wrangle(df):
    def remove_punctuation_and_extra_spaces(df, col_to_wrangle):
        """Standardizes words for getting word count. Eg 'engineer,' becomes 'engineer'."""
        punctuation_pattern = r"([^\w\s]|\n)"
        extra_spaces_pattern = r" +"

        for pattern in [punctuation_pattern, extra_spaces_pattern]:
            df = df.withColumn(
                col_to_wrangle,
                F.regexp_replace(col_to_wrangle, pattern, r" "),
            )

        return df

    for c in df.columns:
        df = df.withColumn(c, F.lower(c))

    for c in ["job_title", "raw_text"]:
        df = remove_punctuation_and_extra_spaces(df, c)

    df = df.withColumn("listing_date", df["listing_date"].cast("date"))

    return df


def get_word_counts(df, col_to_get_word_counts_for):
    df = df.withColumn("words", F.split(F.col(col_to_get_word_counts_for), " "))
    words = df.select(F.explode("words"))
    word_counts = words.groupby("col").count().orderBy("count", ascending=False)
    return word_counts


def get_job_title_counts(df, job_title_list):
    def get_jobs_with_only_data(df_job_title_counts, job_title_list):
        """Titles that include the word 'data' but aren't found in my data-related job titles like 'data engineer', 'data scientist'. Used to find out about data-related job titles not in my list."""

        titles_with_data = [F.col(jt) for jt in job_title_list if "data" in jt]
        data_with_other_word = titles_with_data[1:]

        df_job_title_counts = df_job_title_counts.withColumn(
            "data_combined_row_sum", sum(data_with_other_word)
        )
        df_job_title_counts = df_job_title_counts.withColumn(
            "only_data_found",
            (
                (df_job_title_counts["data"] >= 1)
                & (df_job_title_counts["data_combined_row_sum"] == 0)
            ),
        )

        # only data (sus?)
        return df_job_title_counts.select("job_title").where("only_data_found")

    job_title_counts = df.select("link", "job_title")
    for jt in job_title_list:
        job_title_counts = job_title_counts.withColumn(
            jt, F.regexp_count("job_title", F.lit(jt))
        )

    jobs_with_only_data = get_jobs_with_only_data(job_title_counts, job_title_list)
    frequencies_of_titles = job_title_counts.groupby().sum()

    return job_title_counts, jobs_with_only_data, frequencies_of_titles


def get_tech_counts(df, technologies):
    def remove_spaces(df_tech_counts, technologies):
        """Some postings use eg. power bi while others powerbi. Removing spaces standardizes this."""
        df_tech_counts = df_tech_counts.withColumn(
            "raw_text", F.regexp_replace("raw_text", " ", "")
        )
        technologies = [x.replace(" ", "") for x in technologies]

        return df_tech_counts, technologies

    def get_distinct_sum(df_tech_counts):
        """Only count max one occurrence per posting."""

        df_tech_counts = df_tech_counts.drop("link", "raw_text")
        for c in df_tech_counts.columns:
            # get max(1, value)
            df_tech_counts = df_tech_counts.withColumn(
                c, F.when(F.col(c) == 0, 0).otherwise(1)
            )
        distinct_sum = df_tech_counts.groupby().sum()

        return distinct_sum

    df_tech_counts = df.select("link", "raw_text")
    df_tech_counts, technologies = remove_spaces(df_tech_counts, technologies)

    for search_word in technologies:
        df_tech_counts = df_tech_counts.withColumn(
            search_word,
            F.regexp_count("raw_text", F.lit(search_word)),
        )

    non_distinct_sum = df_tech_counts.groupby().sum()
    distinct_sum = get_distinct_sum(df_tech_counts)

    max_counts_in_single_posting = df_tech_counts.drop("link", "raw_text")
    max_counts_in_single_posting = max_counts_in_single_posting.groupby().max()

    return df_tech_counts, non_distinct_sum, distinct_sum, max_counts_in_single_posting


# %%
df = spark.read.option("inferSchema", "True").parquet(path)
df = wrangle(df)

# links are unique, use them as id
df.select("link").distinct().count() == df.count()

word_counts = []
for c in ["job_title", "raw_text"]:
    word_counts.append(get_word_counts(df, c))

job_title_counts, jobs_with_only_data, frequencies_of_titles = get_job_title_counts(
    df, job_title_list
)

(
    df_tech_counts,
    non_distinct_sum,
    distinct_sum,
    max_counts_in_single_posting,
) = get_tech_counts(df, technologies)

# write to excel
# to_write_df.to_excel("output.xlsx")
