/**
  * Created by marc on 15.04.17.
  */


import scala.slick.driver.PostgresDriver.simple._
import org.apache.spark._

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkContext = setup()
    println("Hello, world!")
    testConnection()
  }

  def testConnection(): Unit = {
    val connectionUrl = "jdbc:postgresql://localhost/my-db?user=postgres&password=pw"

    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
//      implicit session =>
//        val users = TableQuery[Users]
//
//        // SELECT * FROM users
//        users.list foreach { row =>
//          println("user with id " + row._1 + " has username " + row._2)
//        }
//
//        // SELECT * FROM users WHERE username='john'
//        users.filter(_.username === "john").list foreach { row =>
//          println("user whose username is 'john' has id "+row._1 )
//        }
    }
  }

  def setup(): SparkContext = {
    val conf = new SparkConf().setAppName("Name")
    //conf.setMaster()
    val spark = new SparkContext(conf)
    spark
  }
}
