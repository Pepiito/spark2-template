package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exercise3 {
  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    // question 1
    println(toursDF
      .select($"tourDifficulty")
      .distinct()
      .count())

    // question 2
    toursDF
      .agg(
        min($"tourPrice"),
        max($"tourPrice"),
        avg($"tourPrice")
      )
      .show()

    // question 3
    toursDF
        .select($"tourPrice", $"tourDifficulty")
        .groupBy($"tourDifficulty")
        .agg(
          min($"tourPrice"),
          max($"tourPrice"),
          avg($"tourPrice")
        )
        .show()

    // question 4
    toursDF
      .select($"tourPrice", $"tourLength", $"tourDifficulty")
      .groupBy($"tourDifficulty")
      .agg(
        min($"tourPrice"),
        max($"tourPrice"),
        avg($"tourPrice"),
        min($"tourLength"),
        max($"tourLength"),
        avg($"tourLength")
      )
      .show()

    // question 5
    toursDF
      .select(explode($"tourTags"))
      .groupBy($"col")
      .count()
      .orderBy($"count".desc).
      show(10)

    // question 6
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col")
      .count()
      .orderBy($"count".desc)
      .show(10)

    // question 7

    toursDF
      .select($"tourPrice", explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .agg(
        min($"tourPrice"),
        max($"tourPrice"),
        avg($"tourPrice")
      )
      .orderBy($"avg(tourPrice)".desc)
      .show()


  }
}
