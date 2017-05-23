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

  def transformCSVtoRaster(parmeters : Settings): IntArrayTile ={
    //https://www.google.com/maps/place/40%C2%B033'06.6%22N+74%C2%B007'46.0%22W/@40.7201276,-74.0195387,11.25z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.551826!4d-74.129441
    //lat = 40.551826, lon=-74.129441
    //https://www.google.com/maps/place/40%C2%B059'32.5%22N+73%C2%B035'51.3%22W/@40.8055274,-73.8900207,10.46z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.992352!4d-73.597571
    //lat =40.992352, lon=-73.597571
    //lat = lat_min-lat_max = 440526 = 47, lon = lon_min-lon_max =531870 = 50 km => measurements approximately in meters

    //other values https://www.deine-berge.de/Rechner/Koordinaten/Dezimal/40.800296,-73.928375
    //40.800296, -73.928375
    //40.703286, -74.019012

    //sample
    //40.763458, -73.967244
    //40.701915, -74.018704
    val bufferedSource = Source.fromFile(parmeters.inputDirectoryCSV)
    val multiToInt = 1000000
    val shiftToPostive = 74.018704*multiToInt
    val latMin = 40.701915*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
    val lonMin = -74.018704*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
    val latMax = 40.763458*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
    val lonMax = -73.967244*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)
    //2016-02-25 17:24:20
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      val result = new RowTransformationTime(
          lon = (cols(9).toDouble*multiToInt+shiftToPostive).toInt,
          lat = (cols(10).toDouble*multiToInt).toInt,
          time = formatter.parseDateTime(cols(2)))
      result
    }).filter(row => row.lon>lonMin && row.lon<lonMax && row.lat>latMin && row.lat<latMax) //To remove entries not in range
    //.filter(row => (row.time.getHourOfDay> 20 && row.time.getHourOfDay()< 22)) //To look at data range



    val rasterLatLength = ((latMax-latMin)/parmeters.sizeOfRasterLat).ceil.toInt
    val rasterLonLength = ((lonMax-lonMin)/parmeters.sizeOfRasterLon).ceil.toInt
    val tile = IntArrayTile.ofDim(rasterLatLength,rasterLonLength)
    var colIndex = 0
    var rowIndex = 0
    for(row <- file){
      colIndex = ((row.lat-latMin)/parmeters.sizeOfRasterLat).toInt
      rowIndex = ((row.lon-lonMin)/parmeters.sizeOfRasterLon).toInt
      tile.setDouble(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
    }
//    file.map(row => {
//      colIndex = ((row.lat*mulitToCalcWihtInt-latMin)/1000).toInt
//      rowIndex = ((row.lon*mulitToCalcWihtInt-lonMin)/1000).toInt
//      tile.set(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
//    })
    bufferedSource.close()
    tile
  }
}
