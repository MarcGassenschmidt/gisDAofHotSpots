package db

import java.io._
import java.sql.Connection

import geotrellis.raster.io.ascii.FileReader
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, Row}

import scala.io.Source
import scala.slick.driver.PostgresDriver.simple._
/**
  * Created by marc on 17.04.17.
  */
class ImportToDB {


  def copy(): Unit ={
    //For spark
    //https://gist.github.com/longcao/bb61f1798ccbbfa4a0d7b76e49982f84
    val query = "copy test(id, lonpickup, latpickup) FROM STDIN DELIMITER ','"
    val connectionUrl = "jdbc:postgresql://localhost:5432/Test?user=postgres&password=pw"
    val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
    val connection = db.createConnection()
    val cm = new CopyManager(connection.asInstanceOf[BaseConnection])
    cm.copyIn(query, new FileInputStream("/home/marc/media/Downloads/out.csv"))
    connection.close()
    println("Copy finished")

  }

  def editCSVatom(): Unit ={
    val bufferedSource = Source.fromFile("/home/marc/media/Downloads/in.csv")
    val pw = new PrintWriter(new File("/home/marc/media/Downloads/out.csv"))

    for ((line, counter) <- bufferedSource.getLines.zipWithIndex) {
      val cols = line.split(",").map(_.trim)
      if(cols(5)==0.0 || cols(6)==0.0 || counter == 0){
        //continue
      } else {
        pw.println(counter.toString+","+cols(5)+","+cols(6))
      }
//      if(counter>1000) {
//        bufferedSource.close()
//        pw.close()
//        return
//      }
    }
    bufferedSource.close()
    pw.close()
  }









}
