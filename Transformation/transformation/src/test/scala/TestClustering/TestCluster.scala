package TestClustering

import clustering.Cluster
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by marc on 25.04.17.
  */
class TestCluster extends FunSuite with BeforeAndAfter {
  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[1]")
  var spark : SparkContext = _
  before {
    spark = SparkContext.getOrCreate(conf)
  }

  test("Test Spark Configuration"){
    val cluster = new Cluster()
    cluster.test(spark, "/downloads")
  }

  after {
    spark.stop()
  }

}
