package rasterTransformation

import java.io.{File, PrintWriter}

import clustering.Row
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import geotrellis.vector._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
/**
  * Created by marc on 21.04.17.
  */
class Transformation {



  def trasnform(rootPath: Path, config: SparkConf): Unit ={
    val sc = geotrellis.spark.util.SparkUtils.createSparkContext("Test console", config)
    val jobConfig = new JobConf
    val rdd = sc.hadoopRDD(jobConfig,classOf[FileInputFormat[Point,Point]],classOf[Point], classOf[Point])
    

//    /* The `config` argument is optional */
//    val store: AttributeStore = HadoopAttributeStore(rootPath, config)
//
//    val reader = HadoopLayerReader(store)
//    val writer = HadoopLayerWriter(rootPath, store)
  }

  def transformCSVtoRaster(): IntArrayTile ={
    //https://www.google.com/maps/place/40%C2%B033'06.6%22N+74%C2%B007'46.0%22W/@40.7201276,-74.0195387,11.25z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.551826!4d-74.129441
    //lat = 40.551826, lon=-74.129441
    //https://www.google.com/maps/place/40%C2%B059'32.5%22N+73%C2%B035'51.3%22W/@40.8055274,-73.8900207,10.46z/data=!4m5!3m4!1s0x0:0x0!8m2!3d40.992352!4d-73.597571
    //lat =40.992352, lon=-73.597571
    //lat = lat_min-lat_max = 440526 = 47, lon = lon_min-lon_max =531870 = 50 km => measurements approximately in meters
    val bufferedSource = Source.fromFile("/home/marc/media/Downloads/in.csv")
    val multiToInt = 1000000
    val shiftToPostive = 74.407877*multiToInt
    val latMin = 40.376048*multiToInt//Math.max(file.map(row => row.lat).min,40.376048)
    val lonMin = -74.407877*multiToInt+shiftToPostive//Math.max(file.map(row => row.lon).min,-74.407877)
    val latMax = 41.330106*multiToInt//Math.min(file.map(row => row.lat).max,41.330106)
    val lonMax = -73.292793*multiToInt+shiftToPostive//Math.min(file.map(row => row.lon).max,-73.292793)

    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      val result = new RowTransformation(lon = (cols(5).toDouble*multiToInt+shiftToPostive).toInt, lat = (cols(6).toDouble*multiToInt).toInt)
      result
    }).filter(row => row.lon>lonMin && row.lon<lonMax && row.lat>latMin && row.lat<latMax) //To remove entries not in range



    val rasterSize = 100 //1km
    val rasterLatLength = ((latMax-latMin)/rasterSize).ceil.toInt
    val rasterLonLength = ((lonMax-lonMin)/rasterSize).ceil.toInt
    val tile = IntArrayTile.ofDim(rasterLatLength,rasterLonLength)
    var colIndex = 0
    var rowIndex = 0
    for(row <- file){
      colIndex = ((row.lat-latMin)/rasterSize).toInt
      rowIndex = ((row.lon-lonMin)/rasterSize).toInt
      tile.set(colIndex,rowIndex,tile.get(colIndex,rowIndex)+1)
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
