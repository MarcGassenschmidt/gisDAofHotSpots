import org.apache.spark.SparkContext
import org.apache.spark.util.random

/**
  * Created by marc on 13.04.17.
  */
class MapReduce {
  //: org.apache.spark.rdd.RDD[U]
  def map(slices: Int, context: SparkContext): org.apache.spark.rdd.RDD[Int] = {

    val n = 100000 * slices
    return context.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }
  }


  def reduce(mapped : org.apache.spark.rdd.RDD[Int]){
    mapped.reduce(_ + _)
  }

}
