"""IMDB Spark Project

This script allows the user to load IMDB Datasets, print top 10 rows
from each dataset, execute necessary transformations implemented by
separate functions, and store obtained results to `.csv` files (one per
transformation).

This tool accepts `.gz` files of IMDB Datasets (https://datasets.imdbws.com/)
located in the local folder `imdb-data/`.

This script requires that `pyspark` package is installed within the Python
environment you are running this script in (with JDK and PATH variables set
in your environment).

This file is possible to be used as a module and contains the following
functions:
    * get_ukrainian_series_movies_titles - returns all titles of series/movies etc. 
        that are available in Ukrainian
    * get_people_born_in_19th_century - returns the list of people’s names, 
        who were born in the 19th century
    * get_movies_last_more_2_hours - returns titles of all movies that last more than 2 hours
    * get_people_and_characters_played - returns  names of people, corresponding movies/series 
        and characters they played in those films
    * get_top_100_adult_movies_per_region - returns information about how many 
        adult movies/series etc. there are per region
    * get_top_50_tvseries_by_episodes - returns information about how many episodes in each TV Series
    * get_10_most_popular_titles_by_each_decade - returns 10 titles of the most popular 
        movies/series etc. by each decade
    * get_10_most_popular_titles_by_each_genre - returns 10 titles of the most popular 
        movies/series etc. by each genre
    * main - the main function of the script
"""

from pyspark.sql import SparkSession


def get_ukrainian_series_movies_titles(spark):
    """Gets all titles of series/movies etc. that are available in Ukrainian.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            DISTINCT title
                        FROM 
                            title_akas
                        WHERE 
                            region = 'UA'
                        """)


def get_people_born_in_19th_century(spark):
    """Gets the list of people’s names, who were born in the 19th century.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            primaryName
                        FROM 
                            name_basics
                        WHERE 
                            birthYear >= 1801 AND birthYear <= 1900
                        """)


def get_movies_last_more_2_hours(spark):
    """Gets titles of all movies that last more than 2 hours.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            primaryTitle
                        FROM 
                            title_basics
                        WHERE 
                            titleType = 'movie' AND runtimeMinutes > 120
                        """)


def get_people_and_characters_played(spark):
    """Gets names of people, corresponding movies/series and characters they played in those films.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            name_basics.primaryName, 
                            title_basics.primaryTitle,
                            title_principals.characters
                        FROM 
                            (title_basics INNER JOIN title_principals 
                                ON title_basics.tconst = title_principals.tconst) 
                            INNER JOIN name_basics 
                                ON name_basics.nconst = title_principals.nconst
                        WHERE 
                            title_principals.category IN ('actor', 'actress') 
                                AND title_principals.characters != '\\\\N'
                        """)


def get_top_100_adult_movies_per_region(spark):    
    """Gets the top 100 regions with counted adult movies/series etc. per region, 
    ordered from the region with the biggest count to the region with the smallest.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            title_akas.region, 
                            COUNT(title_basics.tconst) AS total
                        FROM 
                            title_akas INNER JOIN title_basics 
                                ON title_basics.tconst = title_akas.titleId
                        WHERE 
                            title_basics.isAdult = 1 
                                AND title_akas.region != '\\\\N'
                        GROUP BY 
                            title_akas.region
                        ORDER BY 
                            total DESC
                        LIMIT 100
                        """)


def get_top_50_tvseries_by_episodes(spark):
    """Gets the top 50 TV Series with counted episodes per series,
    ordered from the TV Series with the biggest quantity of episodes to the smallest.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    return spark.sql("""
                        SELECT 
                            title_basics.primaryTitle, 
                            COUNT(title_episode.tconst) as episodes
                        FROM 
                            title_basics INNER JOIN title_episode 
                                ON title_basics.tconst = title_episode.parentTconst
                        GROUP BY 
                            title_basics.primaryTitle
                        ORDER BY 
                            episodes DESC
                        LIMIT 50
                        """)


def get_10_most_popular_titles_by_each_decade(spark):
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
    
    # create the view of decades created from start years, titles, and ratings
    decades_df = spark.sql("""
                        SELECT
                            CONCAT(
                                FLOOR(title_basics.startYear / 10) * 10, 
                                ' - ', 
                                FLOOR(title_basics.startYear / 10) * 10 + 9
                            ) as decade, 
                            title_basics.primaryTitle, 
                            title_ratings.averageRating
                        FROM
                            title_basics INNER JOIN title_ratings 
                                ON title_basics.tconst = title_ratings.tconst
                        """)
    decades_df.createOrReplaceTempView("decades_view")
    
    # partition 10 top titles by each decade
    return spark.sql("""
                        SELECT *
                        FROM (
                            SELECT 
                                decade, 
                                primaryTitle, 
                                averageRating, 
                                ROW_NUMBER() OVER (
                                    PARTITION BY decade ORDER BY averageRating DESC
                                ) AS row_num
                            FROM 
                                decades_view
                        ) AS sub_query
                        WHERE 
                            row_num <= 10
                        ORDER BY 
                            decade DESC, averageRating DESC
                        """)


def get_10_most_popular_titles_by_each_genre(spark):
    """Gets 10 titles of the most popular movies/series etc. by each genre.

    Parameters
    ----------
    spark : SparkSession
        The entry point for DataFrame and SQL functionality

    Returns
    -------
    DataFrame
        a data frame representing the query result
    """
    
    # create the view of genres exploded from string arrays, titles, and ratings
    genres_df = spark.sql("""
                        SELECT
                            POSEXPLODE(SPLIT(title_basics.genres, ',')) AS (pos, genre), 
                            title_basics.primaryTitle, 
                            title_ratings.averageRating
                        FROM
                            title_basics INNER JOIN title_ratings  
                                ON title_basics.tconst = title_ratings.tconst
                        """)
    genres_df.createOrReplaceTempView("genres_view")
    
    # partition 10 top titles by each genre
    return spark.sql("""
                        SELECT *
                        FROM (
                            SELECT 
                                genre, 
                                primaryTitle, 
                                averageRating, 
                                ROW_NUMBER() OVER (
                                    PARTITION BY genre ORDER BY averageRating DESC
                                ) AS row_num
                            FROM
                                genres_view
                            WHERE
                                genre != '\\\\N'
                        ) AS sub_query
                        WHERE 
                            row_num <= 10
                        ORDER BY 
                            genre DESC, averageRating DESC
                        """)


def main():
    """Extracts data from IMDB Datasets from the `imdb-data` folder, 
    transforms data using declared functions, 
    and loads results to .csv files in the `output/` folder."""
    
    # Setup
    data_sets = {
        "name_basics": "name.basics.tsv.gz",
        "title_akas": "title.akas.tsv.gz",
        "title_basics": "title.basics.tsv.gz",
        "title_cew": "title.crew.tsv.gz",
        "title_episode": "title.episode.tsv.gz",
        "title_principals": "title.principals.tsv.gz",
        "title_ratings": "title.ratings.tsv.gz"
    }
    
    # relative paths to data inputs/outputs
    input_folder = "imdb-data/"
    output_folder = "output/"
    
    # how many records will be displayed to the console
    top_rows = 10
    
    spark = SparkSession.builder.master("local[*]").appName("IMDB").getOrCreate()

    # Extraction
    for name, file in data_sets.items():
        data_frame = spark.read.csv(input_folder + file, header="True", sep="\t")
        data_frame.createOrReplaceTempView(name)
        
        print(f"Dataset: {name}")
        data_frame.printSchema()
        data_frame.show(top_rows)
    
    # Transformation
    tasks = []
    
    print("Task 1: All titles of series/movies etc. that are available in Ukrainian.")
    ukrainian_series_movies_titles = get_ukrainian_series_movies_titles(spark)
    ukrainian_series_movies_titles.show(top_rows)
    tasks.append(ukrainian_series_movies_titles)
    
    print("Task 2: The list of people’s names, who were born in the 19th century.")
    people_born_in_19th_century = get_people_born_in_19th_century(spark)
    people_born_in_19th_century.show(top_rows)
    tasks.append(people_born_in_19th_century)
    
    print("Task 3: Titles of all movies that last more than 2 hours.")
    movies_last_more_2_hours = get_movies_last_more_2_hours(spark)
    movies_last_more_2_hours.show(top_rows)
    tasks.append(movies_last_more_2_hours)
    
    print("Task 4: Names of people, corresponding movies/series and characters they played in those films.")
    people_and_characters_played = get_people_and_characters_played(spark)
    people_and_characters_played.show(top_rows)
    tasks.append(people_and_characters_played)
    
    print("Task 5: Top 100 regions with counted adult movies/series etc. in each.")
    top_100_adult_movies_per_region = get_top_100_adult_movies_per_region(spark)
    top_100_adult_movies_per_region.show(top_rows)
    tasks.append(top_100_adult_movies_per_region)
    
    print("Task 6: Top 50 of TV Series with counted episodes in each.")
    top_50_tvseries_by_episodes = get_top_50_tvseries_by_episodes(spark)
    top_50_tvseries_by_episodes.show(top_rows)
    tasks.append(top_50_tvseries_by_episodes)
    
    print("Task7: 10 titles of the most popular movies/series etc. by each decade.")
    ten_most_popular_titles_by_each_decade = get_10_most_popular_titles_by_each_decade(spark)
    ten_most_popular_titles_by_each_decade.show(top_rows)
    tasks.append(ten_most_popular_titles_by_each_decade)
    
    print("Task 8: 10 titles of the most popular movies/series etc. by each genre.")
    ten_most_popular_titles_by_each_genre = get_10_most_popular_titles_by_each_genre(spark)
    ten_most_popular_titles_by_each_genre.show(top_rows)
    tasks.append(ten_most_popular_titles_by_each_genre)
    
    # Loading
    for task_num in range(0, len(tasks)):
        # write results to task1, task2, ... taskN .csv files
        tasks[task_num].write.format("csv").mode("overwrite").save(f"{output_folder}task{task_num + 1}")


if __name__ == "__main__":
    main()
