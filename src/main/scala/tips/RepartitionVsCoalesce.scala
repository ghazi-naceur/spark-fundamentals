package tips

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RepartitionVsCoalesce {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  val session: SparkContext = spark.sparkContext

  val rdd: RDD[Int] = session.parallelize(1 to 100000)

  // repartition
  val repartitioned: RDD[Int] = rdd.repartition(2)
  repartitioned.count()
  // repartition incurs a shuffle

  // coalesce
  val coalesced: RDD[Int] = rdd.coalesce(2)
  coalesced.count()
  // coalesce does not incur a shuffle


  def main(args: Array[String]): Unit = {
    println(rdd.partitions.length) // "local[*]" => Usage of the maximum number of virtual processors
                                    // => In this case, rdd.partitions.length = maximum number of virtual processors
    println(repartitioned.partitions.length)

    Thread.sleep(100000000) // Just to have enough time to consult the Spark job with Spark UI

    // With Spark UI, we will see that the repartition job took 1 second to be evaluated, but the
    // coalesce job took only 38 ns.
    // When we see the 2 DAGs, we will find out that repartition is taking 2 staging, so it is causing a shuffle.
    // In the other hand, coalesce, it is taking only 1 stage with no shuffle needed.
    // The reason why coalesce is not causing a shuffle because it "stitches" partitions together.
//    Decreasing partitions :
    // repartition will spread data evenly between all the output partitions, but in the case of
    // coalesce, each input partition is only included in exactly one output partition => This will cause a massive
    // performance improvement in the case of coalesce.
//    Increasing partitions :
    // When increasing partitions, coalesce is equal to repartition.

//    The utility of repartition is to uniform data distribution. But coalesce doesn't guarantee that.

  }
}
