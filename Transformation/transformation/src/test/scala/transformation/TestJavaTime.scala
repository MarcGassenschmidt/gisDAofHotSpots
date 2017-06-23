package transformation


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite

/**
  * Created by marc on 10.05.17.
  */
class TestJavaTime extends FunSuite {

  test("Test if Java Time works as expected"){
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dateTime = LocalDateTime.from(formatter.parse("2016-02-25 17:24:20"))
    assert(dateTime.getHour==17)
  }

}
