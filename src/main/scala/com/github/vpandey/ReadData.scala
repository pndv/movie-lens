package com.github.vpandey

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class ReadData {

    private val delimiter = ","
    private val genreDelimiter = """\|"""
    private val spark: SparkSession = SparkSession.builder()
                                      .appName(App.name)
                                      .master(App.sparkMaster)
                                      .getOrCreate()

    private val schemaMovie: StructType = StructType(Array(StructField("movieId", LongType, nullable = false),
                                                           StructField("title", StringType, nullable = false),
                                                           StructField("genres", StringType, nullable = true)))
    private val schemaTags: StructType = StructType(Array(StructField("userIdTag", LongType, nullable = false),
                                                          StructField("movieIdTag", LongType, nullable = false),
                                                          StructField("tag", StringType, nullable = true),
                                                          StructField("timestampTag", LongType, nullable = true)))
    private val schemaLinks: StructType = StructType(Array(StructField("movieIdLink", LongType, nullable = false),
                                                           StructField("imdbId", LongType, nullable = true),
                                                           StructField("tmdbId", LongType, nullable = true)))
    private val schemaRating: StructType = StructType(Array(StructField("userId", LongType, nullable = false),
                                                            StructField("movieIdRating", LongType, nullable = false),
                                                            StructField("rating", FloatType, nullable = false),
                                                            StructField("timestampRating", LongType, nullable = false)))

    def getModifiedSchema(df: DataFrame, modifyColumn: String, idColumn: String, nameColumn: String): StructType = {
        val getUniqueElements: (DataFrame, String) => Set[String] = (df: DataFrame, column: String) => {
            val elementList: Array[String] = for (row <- df.select(column).collect) yield {
                row.getString(0)
            }
            val elements: Array[String] = for {
                g <- elementList
                genres <- g.toLowerCase.split(genreDelimiter).map(_.trim)
            } yield {
                genres
            }

            elements.toSet
        }

        val idField = StructField(idColumn, LongType, nullable = false)
        val nameField = StructField(nameColumn, StringType, nullable = false)
        val uniqueElements: Seq[String] = getUniqueElements(df, modifyColumn).toSeq
        val schemaUniqueElements: Seq[StructField] =
            uniqueElements.foldLeft(Seq.empty[StructField])((seq, col) =>
                                                                seq :+
                                                                StructField(col, BooleanType, nullable = true))

        val schema = StructType(idField +: nameField +: schemaUniqueElements)

        schema
    }

    def readAndConvertFiles(): Unit = {
        import spark.implicits._
        val movie = getMovieFrame

        val link = getLinkFrame

        val tag = getTagsFrame

        val rating = getRatingFrame

        val movieRatings = movie.join(link, movie("movieId") === link("movieIdLink"))
                           .join(tag, movie("movieId") === tag("movieIdTag"))
                           .join(rating, movie("movieId") === rating("movieIdRating"))
                           .drop($"movieIdLink")
                           .drop($"movieIdTag")
                           .drop($"movieIdRating")

        val writer = movieRatings.write

        println("Writing csv...")
        writer.option("header", "true").csv("all.csv")

        println("Writing parquet...")
        writer.parquet("all.parquet")
    }

    def getMovieFrame: DataFrame = {
        import spark.implicits._
        val moviesPath: String = classOf[ReadData].getClassLoader.getResource("data/movies.csv").getPath

        val originalMovieFrame: DataFrame = spark.read
                                            .option("header", "true") // Use first line of all files as header
                                            .option("delimiter", delimiter)
                                            .schema(schemaMovie)
                                            .csv(moviesPath)

        val movieSchema: StructType = getModifiedSchema(originalMovieFrame, "genres", "movieId", "title")
        val movieSchemaSize = movieSchema.fields.length

        val convertRow: Row => Row = (row: Row) => {
            val id: Long = row.getLong(0)
            val title: String = row.getString(1)
            val genres = row.getList[String](2).asScala.toSet
            val parameters: Array[Any] = new Array[Any](movieSchemaSize)
            parameters(0) = id
            parameters(1) = title
            for (i <- 2 until parameters.length) {
                parameters(i) = false
            }

            if (genres.nonEmpty) {
                for (i <- 2 until movieSchemaSize) {
                    val genre: String = movieSchema.fields(i).name
                    parameters(i) = genres.contains(genre)
                }
            }
            Row(parameters: _*)
        }

        val df: DataFrame =
            originalMovieFrame.withColumn("movieId", trim($"movieId").cast(LongType))
            .withColumn("title", trim($"title"))
            .withColumn("genres",
                        split(lower($"genres"), genreDelimiter).cast(ArrayType(StringType, containsNull = true)))

        val rddMovie: RDD[Row] = df.rdd.map(convertRow)
        val movieFrame: DataFrame = spark.createDataFrame(rddMovie, movieSchema)

        movieFrame
    }

    def getTagsFrame: DataFrame = {
        val tags = {
            val tagsPath: String = classOf[ReadData].getClassLoader.getResource("data/tags.csv").getPath

            spark.read
            .option("header", "true") // Use first line of all files as header
            .option("delimiter", ",")
            .schema(schemaTags)
            .csv(tagsPath)
        }
        tags
    }

    def getLinkFrame: DataFrame = {
        val linkFrame: DataFrame = {
            val linksPath: String = classOf[ReadData].getClassLoader.getResource("data/links.csv").getPath

            spark.read
            .option("header", "true") // Use first line of all files as header
            .option("delimiter", delimiter)
            .schema(schemaLinks)
            .csv(linksPath)
        }
        linkFrame
    }

    def getRatingFrame: DataFrame = {
        val ratingFrame: DataFrame = {
            val ratingsPath: String = classOf[ReadData].getClassLoader.getResource("data/ratings.csv").getPath

            spark.read
            .option("header", "true")
            .option("delimiter", delimiter)
            .schema(schemaRating)
            .csv(ratingsPath)
            .na
            .drop
        }
        ratingFrame
    }
}
