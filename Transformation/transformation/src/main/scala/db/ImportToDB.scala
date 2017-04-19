package db

import java.io.{File, PrintWriter}
import java.sql.Connection
import java.io.InputStream

import org.apache.spark.SparkContext
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{ DataFrame, Row }
import scala.io.Source
import scala.slick.driver.PostgresDriver.simple._
/**
  * Created by marc on 17.04.17.
  */
class ImportToDB {

  val jdbcUrl = "jdbc:postgresql://localhost:5432/smallsample?user=postgres&password=pw"

  val connectionProperties = {
    val props = new java.util.Properties()
    props.setProperty("driver", "org.postgresql.Driver")
    props
  }

  val cf: () => Connection = JdbcUtils.createConnectionFactory(jdbcUrl, connectionProperties)

  def editCSV(context: SparkContext): Unit = {

  }

  def copy(): Unit ={
    //For spark
    //https://gist.github.com/longcao/bb61f1798ccbbfa4a0d7b76e49982f84


    val query = "copy test(id, lonpickup, latpickup, londropoff, latdropoff) FROM STDIN DELIMITER ','"
    val connectionUrl = "jdbc:postgresql://localhost:5432/smallsample?user=postgres&password=pw"
    val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
    var connection = db.createConnection()
    //var result = connection.createStatement().execute(query)
    val cm = new CopyManager(connection.asInstanceOf[BaseConnection])
    cm.copyIn(query)
    connection.close()
    println("Copy finisehd")

  }

  def editCSVatom(): Unit ={
    val bufferedSource = Source.fromFile("/home/marc/Downloads/file.csv")
    val pw = new PrintWriter(new File("/home/marc/Downloads/scalaOut.csv"))

    var counter = 0;
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      pw.println(counter.toString+","+cols(5)+","+cols(6)+","+cols(7)+","+cols(8)+",")
      counter += 1
    }
    bufferedSource.close()
    pw.close()
  }









}
