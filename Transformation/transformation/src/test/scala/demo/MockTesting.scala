package demo

import java.sql.Connection

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._


/**
  * Created by marc on 17.04.17.
  */
class MockTesting extends FunSuite with BeforeAndAfter with MockitoSugar{
  ignore ("test login service") {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(null)
  }


}
