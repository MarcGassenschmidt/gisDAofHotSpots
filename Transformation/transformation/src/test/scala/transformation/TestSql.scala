package transformation

import java.sql.Connection

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.slick.driver.PostgresDriver.simple._

/**
  * Created by marc on 17.04.17.
  */
class TestSql extends FunSuite with BeforeAndAfter {

  val connectionUrl = "jdbc:postgresql://localhost:5432/smallsample?user=postgres&password=pw"
  val db = Database.forURL(connectionUrl, driver = "org.postgresql.Driver")
  var connection : Connection = _
//  before {
//    connection = db.createConnection()
//  }

  ignore("Test if Connection exist"){
    println(connection.getCatalog);
    val result = connection.createStatement().executeQuery("select * from test as t where t.id<20")
    assert(result.next()==true)


  }

  test("Test if mapping is Correct") (pending)

  ignore("Test if Connection is Closed"){
    connection.close()
    assert(connection.isClosed)

  }
}
