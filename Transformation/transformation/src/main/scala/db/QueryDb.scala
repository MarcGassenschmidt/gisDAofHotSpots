package db

import java.io.FileInputStream
import java.sql.Connection

import geotrellis.raster._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.vector.Extent

import scala.slick.driver.PostgresDriver.simple._
/**
  * Created by marc on 17.04.17.
  */
class QueryDb {
  def query(context: SparkContext, dbConnection : Connection): Unit ={

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    val dataFrame = sparkSession.sql("select * from test as t")
    dataFrame.foreachPartition { it =>
      val connectionUrl = "jdbc:postgresql://localhost:5432/smallsample?user=postgres&password=pw"
      val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
      val connection = db.createConnection()
      //TODO
    }

  }

  def readGeoTiff(): ArrayTile = {
    val path: String = "/home/marc/media/SS_17/tmp/sf_colors.tif"
    //val e: Extent = Extent(0, 1, 2, 3)
    //val geoTiff: SinglebandGeoTiff = GeoTiffReader.(path, e)
    val geoTiff: SinglebandGeoTiff =SinglebandGeoTiff.streaming(path)

    val arrayTile = geoTiff.toArrayTile()
    arrayTile

  }


  def getRaster(): ArrayTile ={
    val query = "SELECT ST_AsBinary((ST_Union(ST_AsRaster(t.geompickup, 0.1, -0.1, 100, 100, '8BUI')))) rast FROM test as t  where t.id < 10000"
    val statistics = "SELECT ST_SummaryStats((ST_Union(ST_AsRaster(t.geompickup, 0.1, -0.1, 100, 100, '8BUI')))) rast FROM test as t  where t.id < 10000\n"
    val connectionUrl = "jdbc:postgresql://localhost:5432/Test?user=postgres&password=pw"
    val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
    val connection = db.createConnection()
    val res = connection.createStatement().executeQuery(query)
    var tile: ArrayTile = null
    while (res.next()){
      println(res.getBytes(1).length)
      tile = ArrayTile.fromBytes(res.getBytes(1), UByteCellType, 100, 100)

      println(tile.get(0,0))

      println(tile.size)

    }
    connection.close()
    println("Converted to Raster finished")
    tile
  }
}
