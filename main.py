from pyspark.sql import SparkSession

datasets = {"name_basics": {"file": "name.basics.tsv.gz"},
      "title_akas": {"file": "title.akas.tsv.gz"},
      "title_basics": {"file": "title.basics.tsv.gz"},
      "title_cew": {"file": "title.crew.tsv.gz"},
      "title_episode": {"file": "title.episode.tsv.gz"},
      "title_principals": {"file": "title.principals.tsv.gz"},
      "title_ratings": {"file": "title.ratings.tsv.gz"}}


def get_data(spark, path):
    return spark.read.csv(path, header="True", sep="\t")


def print_schema(name, data, sep_len):
    print(f"Dataset: {name}")
    data.printSchema()
    print("-" * sep_len)


if __name__ == "__main__":
    datasets_folder = "imdb-data/"
    separator_length = 50
    data_property = "data"
    
    spark = SparkSession.builder.master("local[*]").appName("IMDB").getOrCreate()

    for dataset_name in datasets.keys():
        dataset_file = datasets[dataset_name]["file"]
        data = get_data(spark, datasets_folder + dataset_file)
        print_schema(dataset_name, data, separator_length)
        datasets[dataset_name][data_property] = data
