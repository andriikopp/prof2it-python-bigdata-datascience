"""IMDB Spark Project

This script allows the user to load IMDB Datasets, print top 10 rows
from each dataset, execute necessary transformations implemented by
separate functions, and store obtained results to `.csv` files (one per
transformation) to the `output2` folder.

This tool accepts `.gz` files of IMDB Datasets (https://datasets.imdbws.com/)
located in the local folder `imdb-data/`.

This script requires that `pyspark` package is installed within the Python
environment you are running this script in (with JDK and PATH variables set
in your environment).

REMARK! This version completes only tasks 2, 4, and 7 as told by mentor 
to fulfil training requirements.
Due to this naming out output folders is a little bit confusing:
e.g. task1 = task 2, task2 = task 4, and task3 = task 7 xD

This file is possible to be used as a module and it contains the following
functions:

    * get_people_born_in_19th_century - returns the list of people’s names, 
        who were born in the 19th century
    * get_people_and_characters_played - returns  names of people, corresponding 
        movies/series and characters they played in those films
    * get_10_most_popular_titles_by_each_decade - returns 10 titles of the most 
        popular movies/series etc. by each decade
    * main - the main function of the script
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, floor, row_number
from pyspark.sql.window import Window


def get_people_born_in_19th_century(name_basics):
    """Gets the list of people’s names, who were born in the 19th century.

    Parameters
    ----------
    name_basics : DataFrame
        The DataFrame representing the name_basics table

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    # select names and filter by birth year
    return name_basics.select("primaryName").where(col("birthYear").between(1801, 1900))


def get_people_and_characters_played(title_basics, title_principals, name_basics):
    """Gets names of people, corresponding movies/series and characters they played in those films.

    Parameters
    ----------
    title_basics : DataFrame
        The DataFrame representing the title_basics table
    title_principals : DataFrame
        The DataFrame representing the title_principals table
    name_basics : DataFrame
        The DataFrame representing the name_basics table

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    # join title_basics and title_principals data frames
    titles_joined = title_basics.join(title_principals, "tconst")
    
    # join previous join result and name_basics
    titles_names_joined = titles_joined.join(name_basics, "nconst")
    
    # select columns, filter categories and characters
    return titles_names_joined.select("primaryName", 
                                      "primaryTitle", 
                                      "characters") \
        .where(col("category").isin("actor", "actress") 
               & (col("characters") != '\\N'))


def get_10_most_popular_titles_by_each_decade(title_basics, title_ratings):
    """Gets 10 titles of the most popular movies/series etc. by each decade.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    # join title_basics and title_ratings
    titles_joined = title_basics.join(title_ratings, "tconst")
    
    # construct decade ranges in the format YYYY - YYYY
    decade_range = concat_ws(
        " - ",
        floor(titles_joined["startYear"] / 10) * 10,
        floor(titles_joined["startYear"] / 10) * 10 + 9
    )
    
    # data frame of decades created using start years, titles, and ratings
    decades_df = titles_joined.select(decade_range.alias("decade"), 
                                      "primaryTitle", 
                                      "averageRating")
    
    # partition by decades, descending order by rating
    decade_partition = Window.partitionBy("decade").orderBy(col("averageRating").desc())
    
    # apply partition to find row numbers
    # additionally filter empty string decades
    ranked_decades_view = decades_df.select(
        "decade", 
        "primaryTitle", 
        "averageRating", 
        row_number().over(decade_partition).alias("row_num")
    ).where(col("decade") != "")
    
    # limit partitions to 10 rows
    top_10_by_decade = ranked_decades_view.where(col("row_num") <= 10)
    
    # descending order by decades and ratings within partitions
    return top_10_by_decade.orderBy(col("decade").desc(), col("averageRating").desc())


def main():
    """Extracts data from IMDB Datasets from the `imdb-data` folder, 
    transforms data using declared functions, 
    and loads results to .csv files in the `output2/` folder."""
    
    # Setup
    # ! used only data sets for tasks 2, 4, and 7 to fulfil training requirements
    data_sets = {
        "name_basics": {"file": "name.basics.tsv.gz", "data": None},
        "title_basics": {"file": "title.basics.tsv.gz", "data": None},
        "title_principals": {"file": "title.principals.tsv.gz", "data": None},
        "title_ratings": {"file": "title.ratings.tsv.gz", "data": None}
    }
    
    # relative paths to data inputs/outputs
    input_folder = "imdb-data/"
    output_folder = "output2/"
    
    # how many records will be displayed to the console
    top_rows = 10
    
    spark = SparkSession.builder.master("local[*]").appName("IMDB").getOrCreate()

    # Extraction
    for data_set_name in data_sets:
        data_frame = spark.read.csv(input_folder + data_sets[data_set_name]["file"], header="True", sep="\t")
        data_sets[data_set_name]["data"] = data_frame
        
        print(f"Dataset: {data_set_name}")
        data_frame.printSchema()
        data_frame.show(top_rows)
    
    # Transformation
    tasks = []
    
    print("Task 2: The list of people’s names, who were born in the 19th century.")
    people_born_in_19th_century = get_people_born_in_19th_century(data_sets["name_basics"]["data"])
    people_born_in_19th_century.show(top_rows)
    tasks.append(people_born_in_19th_century)
    
    print("Task 4: Names of people, corresponding movies/series and characters they played in those films.")
    people_and_characters_played = get_people_and_characters_played(data_sets["title_basics"]["data"],
                                                                    data_sets["title_principals"]["data"],
                                                                    data_sets["name_basics"]["data"])
    people_and_characters_played.show(top_rows)
    tasks.append(people_and_characters_played)
    
    print("Task7: 10 titles of the most popular movies/series etc. by each decade.")
    ten_most_popular_titles_by_each_decade = get_10_most_popular_titles_by_each_decade(data_sets["title_basics"]["data"],
                                                                                       data_sets["title_ratings"]["data"])
    ten_most_popular_titles_by_each_decade.show(top_rows)
    tasks.append(ten_most_popular_titles_by_each_decade)
    
    # Loading
    for task_num in range(0, len(tasks)):
        # write results to task1, task2, ... taskN .csv files
        tasks[task_num].write.format("csv").mode("overwrite").save(f"{output_folder}task{task_num + 1}")


if __name__ == "__main__":
    main()
