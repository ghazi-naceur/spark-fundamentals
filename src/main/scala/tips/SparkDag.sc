import tips.QueryPlan.spark

val example1 = spark.range(1, 100000) // Spark will create an "id" column containing all range values as rows
val multiplied = example1.selectExpr("id * 5 as id")
multiplied.show(false)

/**
Consult http://localhost:4040

You will find the list of completed stages. Enter the first one and you will find the job DAG.
Tasks :
  - WholeStageCodegen task : This task is done when you run computations on DataFrames and generates java code to build
        RDDs. RDD is the fundamental datasource that Spark natively understands.
  - mapPartitionsInternal task : This run a serial computations on the previous RDDs partitions. In our case, multiplying
        elements by 5

 */

val example2 = spark.range(1, 100000, 2)

val split = example2.repartition(9)

split.take(2).foreach(println)


val data1 = spark.range(1, 100000)
val data2 = spark.range(1, 100000, 2)
val data3 = data1.repartition(9)
val data4 = data2.repartition(11)
val data5 = data3.selectExpr("id * 5 as id")
val joined = data5.join(data4, "id")
val sum = joined.selectExpr("sum(id)")
sum.show(false)

/**
 After executing the previous commands and Spark is smart enough to split the last sequence of code on 2 jobs :
    - First Job :
        - Stage 2 : WholeStageCodegen -> Exchange
        - Stage 3 : Exchange -> WholeStageCodegen
            The number of successful tasks is 11/11, so this is data4 (partitioned into 11 partitions)
            data4 will be stored in memory and broadcasted to all executors, where data5 is present (preparing for the join in memory)
            This strategy is decided by Spark on its own, based on the number of elements in the dataset.
    - Second Job :
        - Stage 4 : WholeStageCodegen -> Exchange
        - Stage 5 : Exchange -> WholeStageCodegen -> Exchange :
            The number of successful tasks is 9/9, so this is data3 (partitioned into 9 partitions)
            The second Exchange is the join between data5 and data4
        - Stage 6 : Exchange -> WholeStageCodegen -> mapPartitionsInternal :
            The join is performed in memory
 -=> DAG visualization is a visual description of the computation that Spark will perform, before the action is being called.
      In other terms, Spark predicates in advance what will happen in the computations, when it computes DAGs
 */


val data1 = spark.range(1, 1000000000)
val data2 = spark.range(1, 1000000000, 2)
val data3 = data1.repartition(9)
val data4 = data2.repartition(11)
val data5 = data3.selectExpr("id * 5 as id")
val joined = data5.join(data4, "id")
val sum = joined.selectExpr("sum(id)")
sum.show(false)
 /**
  This example will take more time to be computed because we're dealing with 100.000.000 row.
  Stage 7 and 8 will be computed in parallel.
  When we open the Spark UI, we'll notice that the DAG is more complex this time, containing more stages :
  The first 2 stages (0 and 1) will be computed in parallel because they are not dependent (data1 and data2).
  Stage 2 is linked to Stage 1 and it is partitioned into 9 partitions, so it is data3.
  Stage 3 is linked to Stage 0 and it is partitioned into 11 partitions, so it is data4.

  Stage 2 contains a WholeStageCodegen stage that contains a mapPartitionsRDD, which references a parallel computation in
    all RDDs. In our case, it is "id * 5 as id". The selectExpr is a non-aggregation select, so it can be performed in parallel
    in the same DataFrame, so this doesn't require an Exchange/Shuffle.

  Stage 4 will contain the join operation (The zippedPartitionsRDD will perform that .. It is an internal operation, not called
    explicitly). After that, Spark will perform another Exchange preparing the Stage 5.

  Stage 5 will contain the sum aggregation. So to perform that, Spark will need to get all entries from the DataFrame into
    the same executor.

  */