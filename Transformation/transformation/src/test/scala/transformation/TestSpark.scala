package transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
  * Created by marc on 17.04.17.
  */
class TestSpark extends FunSuite with BeforeAndAfter {
  val conf = new SparkConf().setAppName("Test")
  conf.setMaster("local[1]")
  var spark : SparkContext = _
  before {
    spark = SparkContext.getOrCreate(conf)
  }

  test("Test Spark Configuration"){
    assert(spark.parallelize(1 to 100).map(x=>x).reduce(_ + _)==5050)
  }

  after {
    spark.stop()
  }

}
