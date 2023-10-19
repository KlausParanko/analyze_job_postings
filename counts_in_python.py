# %%
import re
import pandas as pd


def show_counts_for_technologies(technologies, raw_data):
    counts = {}
    for t in technologies:
        counts[t] = len(re.findall(t, raw_data))

    tech_counts = pd.Series(counts).sort_values(ascending=False)
    print(tech_counts)


def does_power_refer_to_power_bi(raw_data):
    print("Parts where the word 'power' occurs:\n\n")
    for x in re.finditer("power", raw_data):
        start, end = x.span()
        start -= 10
        end += 10
        print(raw_data[start:end])


def show_counts_for_all_words(raw_data):
    to_remove = ["*", "[", "(", ")", "]", ".", ","]
    for tr in to_remove:
        raw_data = raw_data.replace(tr, "")

    words = pd.Series(raw_data.split())
    words = words.value_counts()
    print(words)


# %%
with open("raw_data.txt", "r") as fp:
    raw_data = fp.read()
    raw_data = raw_data.lower()

technologies = [
    "azure synapse",
    "docker",
    "pytorch",
    "scikit",
    "tableau",
    re.compile(r"power\s*bi"),
    "matplotlib",
    "parquet",
    "rabbitmq",
    "kubernetes",
    "dbt",
    "snowflake",
    re.compile(r"data\s*bricks"),
    re.compile("(aws|amazon)"),
    "azure",
    "kafka",
    "spark",
    "airflow",
    "luigi",
    "hadoop",
    "cassandra",
    re.compile("(google | gcp)"),
    re.compile("sql"),
    re.compile(r"\ssql"),
    "python",
    "mysql",
    "postgresql",
    "mongodb",
    "cassandra",
    "synapse",
    "teradata",
    re.compile(r"data\s*factory"),
    re.compile(r"data\s*vault"),
    "google cloud",
    re.compile(r"qlic*k"),
    "redshift",
    re.compile(r"big\s*query"),
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
    re.compile(r"(github)?\s*actions"),
]


# show_counts_for_all_words(raw_data)
show_counts_for_technologies(technologies, raw_data)
# does_power_refer_to_power_bi(raw_data)
