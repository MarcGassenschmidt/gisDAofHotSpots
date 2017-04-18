package demo

import java.sql.Connection

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.{SparkConf, SparkContext}

import scala.slick.driver.PostgresDriver.simple._

object Main {
  def helloSentence = "Hello GeoTrellis"

  def main(args: Array[String]): Unit = {
    println(helloSentence)

    //testConnection()
    setup()
  }

  def testConnection(): Unit = {
    val connectionUrl = "jdbc:postgresql://localhost:5432/smallsample?user=postgres&password=pw"
    val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
    val connection = db.createConnection()
    println(connection.getCatalog);
    val result = connection.createStatement().executeQuery("select * from test as t where t.id<20")
    println(result)


  }

  def setup(): SparkContext = {
    val conf = new SparkConf().setAppName("Name")
    //conf.setMaster()
    val spark = new SparkContext(conf)
    spark
  }


}
