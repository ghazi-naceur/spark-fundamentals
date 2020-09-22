package workshop

import org.apache.spark.sql.SparkSession

object Ex31FindLongestSequence {

  def main(args: Array[String]): Unit = {

    /**
    @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-Longest-Sequence.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val rankDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/rank.csv")
    rankDF.show()
    rankDF.createOrReplaceTempView("rankDF")

    import org.apache.spark.sql.expressions.Window
    // DESC Order scores by department
    /**
    val window = Window.partitionBy("ID").orderBy(col("time").desc)
    rankDF.select(col("ID"),
        dense_rank().over(window).alias("rank"),
        max(col("ID")).over(window).alias("highest"))
      .where(col("rank") === 1)
      .drop(col("rank"))
      .orderBy("ID")
      .show()
    */

    
    import org.apache.spark.sql.expressions.Window
    val idsSortedByTime = Window.
      partitionBy("ID").
      orderBy("time")

    val answer = rankDF.
      select(col("ID"), col("time"), rank over idsSortedByTime as "rank").
      groupBy("ID", "time", "rank").
      agg(count("*") as "count")

    answer.show


    spark.sql(
      """ with tb2(select id,time, time-row_number() over(partition by id order by time) rw1 from rankDF),
        |tb3(select id,count(rw1) rw2 from tb2 group by id,rw1)
        |select id, rw2 from tb3 where (id,rw2) in (select id,max(rw2) from tb3 group by id)
        |group by id, rw2 """.stripMargin).show(false)
  }
}
