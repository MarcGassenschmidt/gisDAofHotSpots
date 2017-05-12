package demo

import java.sql.Connection
import java.util

import Export.{SerializeTile, SoHResult, TileVisualizer}
import clustering.{Cluster, ClusterHotSpots}
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.{SparkConf, SparkContext}
import db.{ImportToDB, QueryDb}
import getisOrd.Weight._
import getisOrd.{GetisOrd, GetisOrdFocal, Weight}
import rasterTransformation.Transformation

import scala.slick.driver.PostgresDriver.simple._

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {

    val totalTime = System.currentTimeMillis()
    println(helloSentence)
    val tile = getRaster(false)
    var results = new util.ArrayList[SoHResult]()
    results.add(new SoHResult(tile,))
    //resampleRaster(tile)
    val score = gStar(tile, Weight.Big)
    val chs = new ClusterHotSpots(score)
    println("HotSpots ="+score.toArrayDouble().count(x => x > 2))
    println("Clusters = "+chs.findClusters(5,3)._2)
    val image = new TileVisualizer()
    image.visualCluster(chs.getClusters(1.5,2)._1, Weight.Big+"_cluster_meta_"+tile.rows+"_"+tile.cols)
    //gStarFocal(tile, Weight.Big)
    println("Total Time ="+((System.currentTimeMillis()-totalTime)/1000))
    println("End")
  }

  def resampleRaster(arrayTile: Tile): Unit = {
    var startTime = System.currentTimeMillis()
    val reducedTile = arrayTile.resample(arrayTile.cols / 50, arrayTile.rows / 50)
    println("Time for Downsample with factor 50 =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + reducedTile.cols + "," + reducedTile.rows + ")")
  }

  def creatRaster(): Tile = {
    var startTime = System.currentTimeMillis()
    val transform = new Transformation
    val arrayTile = transform.transformCSVtoRaster()
    println("Time for RasterTransformation =" + ((System.currentTimeMillis() - startTime) / 1000))
    println("Raster Size (cols,rows)=(" + arrayTile.cols + "," + arrayTile.rows + ")")
    arrayTile
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

  def getRaster(fromCSV : Boolean): Tile = {
    if(fromCSV){
      return creatRaster()
    } else {
      //from serilizable
      val serilizer = new SerializeTile("/home/marc/Masterarbeit/outPut/raster")
      return serilizer.read()
    }
  }

  def writeToSerilizable(tile : Tile): Unit ={
    val serilizer = new SerializeTile("/home/marc/Masterarbeit/outPut/raster")
    serilizer.write(tile)
  }

  def gStar(tile : Tile, weight: Weight): Tile = {
    var startTime = System.currentTimeMillis()
    val ort = new GetisOrd(tile, 3, 3)
    println("Time for static G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    var score = ort.gStarComplete()

    ort.createNewWeight(weight)
    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    //println(ort.gStarComplete(arrayTile))
    val image = new TileVisualizer()
    startTime = System.currentTimeMillis()
    image.visualTile(score, weight+"_meta_"+tile.rows+"_"+tile.cols)
    println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    score
  }

  def gStarFocal(tile : Tile, weight: Weight): Unit = {
    var startTime = System.currentTimeMillis()
    val ortFocal = new GetisOrdFocal(tile, 3, 3, 50, weight)
    var score = ortFocal.gStarComplete()
    println("Time for Focal G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    val image = new TileVisualizer()
    image.visualTile(score, weight+"focal_meta_"+tile.rows+"_"+tile.cols)
    println("Time for Focal G* Image=" + ((System.currentTimeMillis() - startTime) / 1000))
  }

}
