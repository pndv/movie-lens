package com.github.vpandey

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class ReadData {

    val spark: SparkSession = SparkSession.builder().appName(App.name).master(App.sparkMaster).getOrCreate()

    case class Movie(movieId: Long, title: String, genres: String)

    def getModifiedSchema(df: DataFrame, modifyColumn: String, otherColumns: String*): StructType = {
        val getUniqueElements: (DataFrame, String) => Set[String] = (df: DataFrame, column: String) => {
            val elementList: Array[String] = for (row <- df.select(column).collect) yield {
                row
                .getString(0)
            }
            val elements: Array[String] = for {
                g ← elementList
                genres ← g.split("\\|")
            } yield {
                genres
            }
            elements.toSet
        }

        var schema: StructType = StructType(Seq.empty[StructField])
        otherColumns.foreach(column =>
            schema = schema.add(name = column,
                                   dataType = StringType,
                                   nullable = true))
        getUniqueElements(df, modifyColumn).foreach(column =>
            schema = schema.add(name = column,
                                   dataType = BooleanType,
                                   nullable = true))
        schema
    }

    def readAndConvertFiles(): Unit = {
        import org.apache.spark.sql.Row
        import spark.implicits._

        val movieFrame: DataFrame = {
            val moviesPath: String = classOf[ReadData].getClassLoader.getResource("data/movies.csv").getPath

            val schema: StructType = StructType(Array(StructField("movieId",
                                                                                   StringType,
                                                                                   nullable = true),
                                                                       StructField("title",
                                                                                      StringType,
                                                                                      nullable = true),
                                                                       StructField("genres",
                                                                                      StringType,
                                                                                      nullable = true)))

            val originalMovieFrame: DataFrame = spark.read
                                                .format("com.databricks.spark.csv")
                                                .option("header", "true") // Use first line of all files as header
                                                .option("delimiter", ",")
                                                .schema(schema)
                                                .load(moviesPath)

            originalMovieFrame.show(10)

            val movieSchema: StructType = getModifiedSchema(originalMovieFrame, "genres", "movieId", "title")

            val movie: RDD[Row] = {
                val fitRowToMovieSchema: Row => Row = (row: Row) => {
                    val id = row(0).asInstanceOf[String]
                    val title = row(1).asInstanceOf[String]
                    val genre = row(2).asInstanceOf[String]
                    val parameters: Array[Any] = new Array[Any](movieSchema.fields.length)
                    parameters(0) = id
                    parameters(1) = title
                    for (i <- 2 until parameters.length) {
                        parameters(i) = false
                    }

                    if (!genre.isEmpty) {
                        val genres: Set[String] = genre.split(",").toSet.map((g: String) => g.trim)
                        for (i <- 2 until movieSchema.fields.length) {
                            val genre: String = movieSchema.fields(i).name
                            parameters(i) = genres.contains(genre)
                        }
                    }
                    Row(parameters: _*)
                }

                val rdd = originalMovieFrame.rdd
                rdd.map(fitRowToMovieSchema)
            }

            spark.createDataFrame(movie, movieSchema)
        }
        movieFrame.show(20)

        val linkFrame: DataFrame = {
            val linksPath: String = classOf[ReadData].getClassLoader.getResource("data/links.csv").getPath
            val schema: StructType = StructType(Array(StructField("movieId", LongType, nullable = true),
                                                              StructField("imdbId", LongType, nullable = true),
                                                              StructField("tmdbId", LongType, nullable = true)))

            spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("delimiter", ",")
            .schema(schema)
            .load(linksPath)
        }
        linkFrame.show(20)

        val tags = {
            val tagsPath: String = classOf[ReadData].getClassLoader.getResource("data/tags.csv").getPath
            val schema: StructType = StructType(Array(StructField("userId", LongType, nullable = true),
                                                         StructField("movieId", LongType, nullable = true),
                                                         StructField("tag", StringType, nullable = true),
                                                         StructField("timestamp",
                                                                        LongType,
                                                                        nullable = true)))

            spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("delimiter", ",")
            .schema(schema)
            .load(tagsPath)
        }
        tags.show(10)

        val ratingFrame: DataFrame = {
            val ratingsPath: String = classOf[ReadData].getClassLoader.getResource("data/ratings.csv").getPath
            val schema: StructType = StructType(Array(StructField("userId", LongType, nullable = true),
                                                                StructField("movieId", LongType, nullable = true),
                                                                StructField("rating", FloatType, nullable = true),
                                                                StructField("timestamp", LongType, nullable = true)))

            spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("delimiter", ",")
            .schema(schema)
            .load(ratingsPath)
        }
        ratingFrame.show(20)

    }
}
