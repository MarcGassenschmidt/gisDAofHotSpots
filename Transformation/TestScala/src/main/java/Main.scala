/**
  * Created by marc on 11.04.17.
  */
import geotrellis.raster._
import geotrellis.slick._
import org.apache.spark._
import org.apache.spark.util.random
import slick.driver.PostgresDriver
import scala.slick.driver.PostgresDriver.simple._

object Main {
  def
  main(args: Array[String]) {
    val spark: SparkContext = setup
    val slices = if (args.length > 0) args(0).toInt else 2
    //val driver = PostgresDriver
    testConnection()

    val reduce = new MapReduce()
    val result = reduce.map(slices, spark)
    reduce.reduce(result)





    spark.stop()
  }

  def testConnection(): Unit = {
    val connectionUrl = "jdbc:postgresql://localhost/my-db?user=postgres&password=pw"

    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val users = TableQuery[Users]

        // SELECT * FROM users
        users.list foreach { row =>
          println("user with id " + row._1 + " has username " + row._2)
        }

        // SELECT * FROM users WHERE username='john'
        users.filter(_.username === "john").list foreach { row =>
          println("user whose username is 'john' has id "+row._1 )
        }
    }
  }

  def setup: SparkContext = {
    val conf = new SparkConf().setAppName("Name")
    //conf.setMaster()
    val spark = new SparkContext(conf)
    spark
  }
}
