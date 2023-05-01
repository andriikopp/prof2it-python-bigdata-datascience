import os

os.environ["SPARK_HOME"] = "C:\spark-3.4.0-bin-hadoop3"
os.environ["HADOOP_HOME"] = "C:\spark-3.4.0-bin-hadoop3"
os.environ["JAVA_HOME"] = "C:\Program Files\AdoptOpenJDK\jdk-11.0.11.9-hotspot"

from pyspark.sql import SparkSession

datasets = {
    "name_basics": {"file": "name.basics.tsv.gz"},
    "title_akas": {"file": "title.akas.tsv.gz"},
    "title_basics": {"file": "title.basics.tsv.gz"},
    "title_cew": {"file": "title.crew.tsv.gz"},
    "title_episode": {"file": "title.episode.tsv.gz"},
    "title_principals": {"file": "title.principals.tsv.gz"},
    "title_ratings": {"file": "title.ratings.tsv.gz"}
}


def get_ukrainian_series_movies_titles(spark, data):
    """1. Get all titles of series/movies etc. that are available in Ukrainian"""
    data.createOrReplaceTempView("akas")
    return spark.sql("""SELECT DISTINCT title 
                     FROM akas 
                     WHERE region = 'UA'""")


def get_people_born_in_19th_century(spark, data):
    """2. Get the list of people’s names, who were born in the 19th century"""
    data.createOrReplaceTempView("names")
    return spark.sql("""SELECT primaryName
                     FROM names
                     WHERE birthYear >= 1801 AND birthYear <= 1900""")


if __name__ == "__main__":
    # Setup
    input_folder = "imdb-data/"
    output_folder = "output/"
    
    top_rows_display = 10
    separator_length = 50
    
    file_property = "file"
    data_property = "data"
    
    spark = SparkSession.builder.master("local[*]").appName("IMDB").getOrCreate()

    # Extraction
    for dataset_name in datasets.keys():
        dataset_file = datasets[dataset_name][file_property]
        data = spark.read.csv(input_folder + dataset_file, header="True", sep="\t")
        
        print(f"Dataset: {dataset_name}")
        
        data.printSchema()
        data.show(top_rows_display)
        
        print("-" * separator_length)
        
        datasets[dataset_name][data_property] = data
    
    # Transformation
    tasks = []
    
    ukrainian_series_movies_titles = get_ukrainian_series_movies_titles(spark, datasets["title_akas"][data_property])
    print("1. Get all titles of series/movies etc. that are available in Ukrainian")
    ukrainian_series_movies_titles.show(top_rows_display)
    tasks.append(ukrainian_series_movies_titles)
    
    people_born_in_19th_century = get_people_born_in_19th_century(spark, datasets["name_basics"][data_property])
    print("2. Get the list of people’s names, who were born in the 19th century")
    people_born_in_19th_century.show(top_rows_display)
    tasks.append(people_born_in_19th_century)
    
    # Loading
    for task_num in range(0, len(tasks)):
        tasks[task_num].write.format("csv").mode("overwrite").save(f"{output_folder}task{task_num + 1}")
