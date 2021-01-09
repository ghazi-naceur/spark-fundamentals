package tips

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.lang

object QueryPlan {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {

    println("First example :")
    val example1: Dataset[lang.Long] = spark.range(1, 100000)
    val result1: DataFrame = example1.selectExpr("id * 5 as id")// column id multiple by 5, and result will be under a column called id
    result1.explain // Calling the Query Plan : Will show the Physical Plan (final plan), which will be run on executors
    // Reading and understanding the Physical Plan could help you to predicate the performance bottlenecks on your job.
    /**
    Result of 'times5.explain()':
    == Physical Plan ==
      *(1) Project [(id#0L * 5) AS id#2L]
    +- *(1) Range (1, 100000, step=1, splits=8)

    The Query Plan can be read from bottom to top. So :
    Step-1 : the first step is that Spark is creating a range object from
        1 to 100000 [(1) Range (1, 100000, step=1, splits=8)], with a step of "1" (1,2,3...100000) [step=1],
        and the range dataframe is split into 8 partitions [splits=8]
    Step-2 : the second step is Project which is the mathematical term of Database or a DataFrame Select, so we're selecting
        the column "id" (which is an identifier 0 Long), times 5 [(id#0L * 5) AS id#2L], and we're assigning that name
        to a column with the same name "id", but this column has a different identifier "2L" [AS id#2L]
    -=> Step-2 is the final computation
    -=> Step-2 depends on the creation of Step-1 (on other terms, Step-1 is done first).
     */
    println("Second example :")
    val example2: Dataset[lang.Long] = spark.range(1, 100000, 2) // 2 is the number of steps/jumps starting from 1 to 100000
    val result2 = example2.repartition(9)
    result2.explain
    /**
    Explain action's result is :
    == Physical Plan ==
      Exchange RoundRobinPartitioning(9)
    +- *(1) Range (1, 100000, step=2, splits=8)

    Explain action's explanation :
    Step-1 : [+- *(1) Range (1, 100000, step=2, splits=8)], meaning creating a Range from 1 to 100000, with a step of 2, split
        on 8 partitions
    Step-2 : Exchange RoundRobinPartitioning(9) : Exchange means that the operation is a Shuffle, which means that data
        is moved between nodes within the cluster. So the bigger your cluster is, and the bigger your data is, this shuffle/exchange
        operation will take a long time -=> This operation is expensive
        We need to watch out these Exchange operations in our Physical Plan, because it predicts Bottleneck performances.
        In our case, the Exchange operation is of type RoundRobinPartitioning, with the argument 9, meaning that data is partitioned
        evenly across the cluster into 9 partitions.
        -=> Whenever you see an Exchange, it is a Bottleneck performance
    */

    println("Third example :")
    val example3: Dataset[lang.Long] = spark.range(1, 100000)
    val example4: Dataset[lang.Long] = spark.range(1, 100000, 2)
    val partitionedExample3 = example3.repartition(9)
    val partitionedExample4 = example4.repartition(11)

    val example5 = partitionedExample3.selectExpr("id * 5 as id")

    val joined = example5.join(partitionedExample4, "id") // inner join : deduplication of the join column

    val sum = joined.selectExpr("sum(id)")

    sum.explain
    /**
    Result :
    == Physical Plan ==
      Exchange RoundRobinPartitioning(9)
    +- *(1) Range (1, 100000, step=2, splits=8)
    Third example :
      == Physical Plan ==
        *(4) HashAggregate(keys=[], functions=[sum(id#15L)])
    +- Exchange SinglePartition
    +- *(3) HashAggregate(keys=[], functions=[partial_sum(id#15L)])
    +- *(3) Project [id#15L]
    +- *(3) BroadcastHashJoin [id#15L], [id#11L], Inner, BuildRight
      :- *(3) Project [(id#9L * 5) AS id#15L]
      :  +- Exchange RoundRobinPartitioning(9)
      :     +- *(1) Range (1, 100000, step=1, splits=8)
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
          +- Exchange RoundRobinPartitioning(11)
            +- *(2) Range (1, 100000, step=2, splits=8)

    Explanation :
    (1), (2), (3) and (4): are stages which determines which of the previous lines in the result will be performed
    * Stage-1 : [+- *(1) Range (1, 100000, step=1, splits=8)]
        Constructing a range from 1 to 100000, with 1 step jump, split on 8 partitions
    * Between Step-1 and Step-3, there is an Exchange :
          :- *(3) Project [(id#9L * 5) AS id#15L]
          :  +- Exchange RoundRobinPartitioning(9)
            :     +- *(1) Range (1, 100000, step=1, splits=8)
     -=> This Exchange is an intentional Shuffle because we're doing repartitioning (.repartition(9)).

     * Stage-2 :
           (2) Range (1, 100000, step=2, splits=8)
     * If we get "Exchange hashpartitioning(....)", this means that this shuffle is not intentional
        It is set by Spark, to prepare the dataframe for the join operation

     * Stage-3 : The Join with "BroadcastHashJoin" and the result will be get by projecting [Project [(id#9L * 5) AS id#15L] ]
        We will project on a specific column, which will ensure removing deduplication of columns
        [(3) HashAggregate(keys=[], functions=[partial_sum(id#15L)])]: In this particular stage, Spark will run this operation
          on every single partition of the Joined dataset -=> No shuffles yet
     * Between Stage 3 and Stage 4, there is a shuffle/exchange to perform the whole "sum" :
        *(4) HashAggregate(keys=[], functions=[sum(id#15L)])
            +- Exchange SinglePartition
               +- *(3) HashAggregate(keys=[], functions=[partial_sum(id#15L)])
        -=> With this Exchange we will bring all partial sums in a single partition, so that Spark will do a final
            HashAggregate operation with the "sum" operator with the same identifier column

     */
  }
}
