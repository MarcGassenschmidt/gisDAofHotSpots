package transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import parmeters.Settings
import rasterTransformation.Transformation


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


  ignore("Test Spark Rastertransformation") {
    val transform = new Transformation()
    val para = new Settings()
    para.sizeOfRasterLat = 4000
    para.sizeOfRasterLon = 4000
    para.rasterLatLength = ((para.latMax-para.latMin)/para.sizeOfRasterLat).ceil.toInt
    para.rasterLonLength = ((para.lonMax-para.lonMin)/para.sizeOfRasterLon).ceil.toInt
    val tile = transform.transformOneFile(para.inputDirectoryCSV, conf, para)
    println(tile.asciiDraw())
    println(tile.rows)
    println(tile.cols)
  }

  after {
    spark.stop()
  }

}
