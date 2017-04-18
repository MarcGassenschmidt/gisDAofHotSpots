package db

import java.sql.Connection

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
}
