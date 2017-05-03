package db

import java.io.FileInputStream
import java.sql.Connection

import geotrellis.raster._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

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



  def getRaster(): ArrayTile ={
    val query = "SELECT ST_Tile((ST_Union(ST_AsRaster(t.geompickup, 0.1, -0.1, 10, 10, '4BUI'))),300,300) rast FROM test as t  where t.id < 100"
    val connectionUrl = "jdbc:postgresql://localhost:5432/Test?user=postgres&password=pw"
    val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
    val connection = db.createConnection()
    val res = connection.createStatement().executeQuery(query)
    var tile: ArrayTile = ???
    while (res.next()){
      val byteArray = res.getArray(1).toString.getBytes

      var tile = ArrayTile.fromBytes(byteArray.map(c => c.toChar.getNumericValue.toByte), UByteCellType, 300, 300)

      println(tile.get(0,0))
      println(tile.size)

    }
    connection.close()
    println("Converted to Raster finished")
    tile
  }
}
