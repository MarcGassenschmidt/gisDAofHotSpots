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
    val paraChild = new parmeters.Parameters()
    val soh = new SoH()
    val outPutResults = ListBuffer[SoHResult]()
    val outPutResultPrinter = new SoHResultTabell()
    paraChild.weightMatrix = Weight.One
    val totalTime = System.currentTimeMillis()
    println(helloSentence)
    val tile = getRaster(para)
    var results = new util.ArrayList[SoHResult]()


    //results.add(new SoHResult(tile))
    //resampleRaster(tile)
    val score = gStar(tile, para, paraChild)

    val chs = ((new ClusterHotSpots(score._1)).findClusters(para.critivalValue,para.critivalValue),
              (new ClusterHotSpots(score._2)).findClusters(paraChild.critivalValue,paraChild.critivalValue))
    //println("HotSpots ="+score._1.toArrayDouble().count(x => x > 2))

    val image = new TileVisualizer()
    image.visualCluster(chs._1, para.weightMatrix+"_cluster_meta_"+tile.rows+"_"+tile.cols)
    image.visualCluster(chs._2, paraChild.weightMatrix+"_cluster_meta_"+tile.rows+"_"+tile.cols)
    println(chs._1._1.rows, chs._1._1.cols)
    val sohVal :(Double,Double) = soh.getSoHDowAndUp(chs)
    outPutResults += new SoHResult(chs._1._1,
      chs._2._1,
      para.weightMatrix,
      paraChild.weightMatrix,
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
    val serilizer = new SerializeTile("/home/marc/Masterarbeit/outPut/raster")
    if(para.fromFile){
      val raster = creatRaster(para)
      serilizer.write(raster)
      return raster
    } else {
      return serilizer.read()
    }
  }

  def writeToSerilizable(tile : Tile): Unit ={
    val serilizer = new SerializeTile("/home/marc/Masterarbeit/outPut/raster")
    serilizer.write(tile)
  }

  def gStar(tile : Tile, paraParent : parmeters.Parameters, child : parmeters.Parameters): (Tile, Tile) = {
    var startTime = System.currentTimeMillis()
    val ort = new GetisOrd(tile, 3, 3)
    println("Time for static G* values =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    var score =ort.getGstartForChildToo(paraParent, child)



    println("Time for G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    //println(ort.gStarComplete(arrayTile))
    val image = new TileVisualizer()
    startTime = System.currentTimeMillis()
    image.visualTileOld(score._1, paraParent.weightMatrix+"_meta_"+tile.rows+"_"+tile.cols)
    image.visualTileOld(score._2, child.weightMatrix+"_meta_"+tile.rows+"_"+tile.cols)
    println("Time for Image G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    score
  }

  def gStarFocal(tile : Tile, para : parmeters.Parameters): Unit = {
    var startTime = System.currentTimeMillis()
    val ortFocal = new GetisOrdFocal(tile, 3, 3, 50, para.weightMatrix)
    var score = ortFocal.gStarComplete()
    println("Time for Focal G* =" + ((System.currentTimeMillis() - startTime) / 1000))
    startTime = System.currentTimeMillis()
    val image = new TileVisualizer()
    image.visualTile(score, para.weightMatrix+"focal_meta_"+tile.rows+"_"+tile.cols)
    println("Time for Focal G* Image=" + ((System.currentTimeMillis() - startTime) / 1000))
  }

}
