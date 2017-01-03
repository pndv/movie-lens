package com.github.vpandey

/**
  * Hello world!
  *
  */
object App {
    val name        = "movie-lens"
    val sparkMaster = "local[*]"

    def main(args: Array[String]): Unit = {
        val dataReader = new ReadData
        dataReader.readAndConvertFiles()
    }
}
