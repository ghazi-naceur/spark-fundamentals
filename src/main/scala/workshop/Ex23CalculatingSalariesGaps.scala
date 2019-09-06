package workshop

import org.apache.spark.sql.SparkSession

object Ex23CalculatingSalariesGaps {

  def main(args: Array[String]): Unit = {

    /**
    @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Calculating-Gap-Between-Current-And-Highest-Salaries-Per-Department.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val peopleDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/people.csv")
    peopleDF.show()

    val minMaxByDepartment = peopleDF.groupBy( "department").agg("salary" -> "max", "salary" -> "min")
    minMaxByDepartment.show()

    val peopleWithMinMax = peopleDF.join(minMaxByDepartment, "department")
    peopleWithMinMax.show()

    val peopleWithDiff = peopleWithMinMax.withColumn("diff", col("max(salary)") - col("salary"))
    peopleWithDiff.show()

    val result = peopleWithDiff.drop("max(salary)", "min(salary)")
      .orderBy( "department", "id")
      .select("id", "name", "department", "salary", "diff")
    result.show()
  }
}
