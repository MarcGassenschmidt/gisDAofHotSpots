package demo

import java.sql.Connection

import clustering.Cluster
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.{SparkConf, SparkContext}
import db.{ImportToDB, QueryDb}
import gisOrt.GetisOrt
import rasterTransformation.Transformation

import scala.slick.driver.PostgresDriver.simple._

object Main {
  def helloSentence = "Hello GeoTrellis"

  def main(args: Array[String]): Unit = {
    println(helloSentence)
    val transform = new Transformation
    var startTime = System.currentTimeMillis()/1000
    val arrayTile = transform.transformCSVtoRaster()
    val ort = new GetisOrt()
    println("Time for RasterTransformation ="+(System.currentTimeMillis()/1000-startTime))
 //   println(arrayTile.asciiDraw())
//    val tile = db.getRaster()
    startTime = System.currentTimeMillis()
    ort.gStarComplete(arrayTile)
    println("Time for G* ="+((System.currentTimeMillis()-startTime)/1000))
    //println(ort.gStarComplete(arrayTile))


    println("End")
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
    val conf = new SparkConf().setAppName("Cluster 1")
    val spark = new SparkContext(conf)
    spark

  }


}
