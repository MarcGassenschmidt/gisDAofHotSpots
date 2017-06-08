package rasterTransformation

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import clustering.Row
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import geotrellis.vector._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import parmeters.Settings

import scala.collection.immutable.TreeMap
import scala.io.Source
/**
  * Created by marc on 21.04.17.
  */
class Transformation {


  def transformOneFileOld(rootPath: String, config: SparkConf, para : Settings): DoubleArrayTile ={
    val spark = SparkSession.builder.config(config).getOrCreate()


    val files = spark.read.format("CSV").option("header","true").option("delimiter", ",").textFile(rootPath)

    val tile = DoubleArrayTile.ofDim(para.rasterLatLength,para.rasterLonLength)

    tile
  }

  def transformOneFile(rootPath: String, config: SparkConf, para : Settings): IntArrayTile ={
    val sc = SparkContext.getOrCreate(config)
    val files = sc.textFile(rootPath)
    val tile = IntArrayTile.ofDim(para.rasterLatLength,para.rasterLonLength)
    println(files.count())
    val file = files.map(line => line.drop(1)).map(line => {
      val cols = line.split(",").map(_.trim)
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      val result = new RowTransformationTime(
        lon = (cols(9).toDouble*para.multiToInt+para.shiftToPostive).toInt,
        lat = (cols(10).toDouble*para.multiToInt).toInt,
        time = formatter.parseDateTime(cols(2)))
      result
    })//.filter(row => row.lon>para.lonMin && row.lon<para.lonMax && row.lat>para.latMin && row.lat<para.latMax)

    file.map(row => {
      val colIndex = ((row.lat-para.latMin)/para.sizeOfRasterLat).toInt
      val rowIndex = ((row.lon-para.lonMin)/para.sizeOfRasterLon).toInt
      tile.set(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
    })
    tile
  }

  def transformCSVtoRaster(settings : Settings): IntArrayTile ={
    //https://www.google.com/maps/place/40%C2%B033'06.6%22N+74%C2%B007'46.0%22W/@40.7201276,-74.0195387,11.25z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.551826!4d-74.129441
    //lat = 40.551826, lon=-74.129441
    //https://www.google.com/maps/place/40%C2%B059'32.5%22N+73%C2%B035'51.3%22W/@40.8055274,-73.8900207,10.46z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.992352!4d-73.597571
    //lat =40.992352, lon=-73.597571
    //lat = lat_min-lat_max = 440526 = 47, lon = lon_min-lon_max =531870 = 50 km => measurements approximately in meters

    //other values https://www.deine-berge.de/Rechner/Koordinaten/Dezimal/40.800296,-73.928375
    //40.800296, -73.928375
    //40.703286, -74.019012

   // val bufferedSource = Source.fromFile(settings.inputDirectoryCSV+settings.csvYear+"_"+settings.csvMonth+".csv")
    transformCSVtoRasterParametrised(settings,settings.inputDirectoryCSV+"in.csv",5,6,2)
  }


  def transformCSVtoRasterParametrised(settings : Settings, fileName : String, indexLon : Int, indexLat : Int, indexDate : Int): IntArrayTile ={
    val bufferedSource = Source.fromFile(fileName)
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      val result = new NotDataRowTransformation(0,0,null,true)
      if(cols.length>Math.max(indexDate,Math.max(indexLat,indexLon))){
        result.lon = (cols(indexLon).toDouble*settings.multiToInt+settings.shiftToPostive).toInt
        result.lat = (cols(indexLat).toDouble*settings.multiToInt).toInt
        result.time = formatter.parseDateTime(cols(indexDate))
        result.data = false
      }
      result
    }).filter(row => row.lon>=settings.lonMin && row.lon<=settings.lonMax && row.lat>=settings.latMin && row.lat<=settings.latMax && row.data==false)

    val rasterLatLength = ((settings.latMax-settings.latMin)/settings.sizeOfRasterLat).toInt
    val rasterLonLength = ((settings.lonMax-settings.lonMin)/settings.sizeOfRasterLon).toInt
    val tile = IntArrayTile.ofDim(rasterLonLength,rasterLatLength)
    var colIndex = 0
    var rowIndex = 0
    for(row <- file){
      colIndex = ((row.lon-settings.lonMin)/settings.sizeOfRasterLon).toInt
      rowIndex = tile.rows-((row.lat-settings.latMin)/settings.sizeOfRasterLat).toInt-1
      if(rowIndex >= 0 && colIndex >=0 && colIndex < tile.cols && rowIndex < tile.rows){
        tile.set(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
      } else {
        //println("Not in range")
      }

    }
    bufferedSource.close()
    tile
  }
}
