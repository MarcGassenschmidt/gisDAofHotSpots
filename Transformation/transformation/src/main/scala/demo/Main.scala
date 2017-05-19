package demo

import java.sql.Connection
import java.util

import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import clustering.{Cluster, ClusterHotSpots}
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.{SparkConf, SparkContext}
import db.{ImportToDB, QueryDb}
import getisOrd.Weight._
import getisOrd.{GetisOrd, GetisOrdFocal, SoH, Weight}
import rasterTransformation.Transformation

import scala.collection.mutable.ListBuffer
import scala.slick.driver.PostgresDriver.simple._

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {
    val para = new parmeters.Parameters()
    para.weightCols = 10
    para.weightRows = 10
    val paraChild = new parmeters.Parameters()
    paraChild.parent = false
    paraChild.weightCols = 5
    paraChild.weightRows = 5
    val soh = new SoH()
    val outPutResults = ListBuffer[SoHResult]()
    val outPutResultPrinter = new SoHResultTabell()

    val totalTime = System.currentTimeMillis()
    println(helloSentence)
    val tile = getRaster(para)
    var results = new util.ArrayList[SoHResult]()


    //results.add(new SoHResult(tile))
    //resampleRaster(tile)
    val score = gStar(tile, para, paraChild)
    println(score._2.histogram)
    val chs = ((new ClusterHotSpots(score._1)).findClusters(para.critivalValue,para.critivalValue),
              (new ClusterHotSpots(score._2)).findClusters(paraChild.critivalValue,paraChild.critivalValue))
    //println("HotSpots ="+score._1.toArrayDouble().count(x => x > 2))

    val image = new TileVisualizer()
    image.visualTileNew(chs._1._1, para, "cluster")
    image.visualTileNew(chs._2._1, para, "cluster")
    println(chs._1._1.rows, chs._1._1.cols)
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(chs)
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      para,
      paraChild,
      ((System.currentTimeMillis()-totalTime)/1000),
      sohVal)
    println(outPutResultPrinter.printResults(outPutResults))
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

  def creatRaster(para : parmeters.Parameters): Tile = {
    var startTime = System.currentTimeMillis()
    val transform = new Transformation

    val arrayTile = transform.transformCSVtoRaster(para)
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

  def getRaster(para : parmeters.Parameters): Tile = {
    val serilizer = new SerializeTile(para.serilizeDirectory)
    if(para.fromFile){
      val raster = creatRaster(para)
      serilizer.write(raster)
      return raster
    } else {
      return serilizer.read()
    }
  }

  def writeToSerilizable(tile : Tile, para : parmeters.Parameters): Unit ={
    val serilizer = new SerializeTile(para.serilizeDirectory)
    serilizer.write(tile)
  }

  def gStar(tile : Tile, paraParent : parmeters.Parameters, child : parmeters.Parameters): (Tile, Tile) = {
    var startTime = System.currentTimeMillis()
    var ort : GetisOrd = null
    if(paraParent.focal){
      ort = new GetisOrdFocal(tile, 3, 3, paraParent.focalRange)
    } else {
      ort = new GetisOrd(tile, 3, 3)
    }
    println("Time for G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    var score =ort.getGstartForChildToo(paraParent, child)
    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    val image = new TileVisualizer()
    startTime = System.currentTimeMillis()
    image.visualTileNew(score._1, paraParent, "gStar")
    image.visualTileNew(score._2, child, "gStar")
    println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    score
  }



}
