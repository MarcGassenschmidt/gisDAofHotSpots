package demo

import java.sql.Connection

import Export.TileVisualizer
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
    val totalTime = System.currentTimeMillis()
    println(helloSentence)
    val transform = new Transformation
    var startTime = System.currentTimeMillis()
    val arrayTile = transform.transformCSVtoRaster()
    println("Time for RasterTransformation ="+((System.currentTimeMillis()-startTime)/1000))
    println("Raster Size (cols,rows)=("+arrayTile.cols+","+arrayTile.rows+")")
    startTime = System.currentTimeMillis()
    val reducedTile = arrayTile.resample(arrayTile.cols/50, arrayTile.rows/50)
    println("Time for Downsample with factor 50 ="+((System.currentTimeMillis()-startTime)/1000))
    println("Raster Size (cols,rows)=("+reducedTile.cols+","+reducedTile.rows+")")
    startTime = System.currentTimeMillis()
    val ort = new GetisOrt(arrayTile, 3,3)
    println("Time for static G* values ="+((System.currentTimeMillis()-startTime)/1000))

//    println(arrayTile.asciiDraw())
//    val tile = db.getRaster()
    startTime = System.currentTimeMillis()
    val score = ort.gStarComplete()
    println("Time for G* ="+((System.currentTimeMillis()-startTime)/1000))
    //println(ort.gStarComplete(arrayTile))
    val image = new TileVisualizer()
    image.visualTile(score)
    println("Total Time because maybe of lazy evaluations ="+((System.currentTimeMillis()-totalTime)/1000))
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
