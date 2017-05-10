package transformation

import org.joda.time.format.DateTimeFormat
import org.scalatest.FunSuite

/**
  * Created by marc on 10.05.17.
  */
class TestJoda extends FunSuite {

  test("Test if Yoda works as expected"){
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dateTime = formatter.parseDateTime("2016-02-25 17:24:20")
    assert(dateTime.getHourOfDay==17)
  }

}
