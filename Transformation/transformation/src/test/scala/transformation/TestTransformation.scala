package transformation

import java.io.{File, PrintWriter}

import org.scalatest.FunSuite
import parmeters.Settings
import rasterTransformation.Transformation

/**
  * Created by marc on 07.06.17.
  */
class TestTransformation extends FunSuite{

  test("Test CSV Transformation"){
    val settings = new Settings()
    settings.latMin = 0
    settings.lonMin = 0
    settings.latMax = 100
    settings.lonMax = 100
    settings.sizeOfRasterLat = 10
    settings.sizeOfRasterLon = 10
    settings.multiToInt = 1
    settings.shiftToPostive = 0
    val fileName = "/tmp/test.csv"
    val testFile = new File(fileName)
    val writer = new PrintWriter(testFile)
    writer.println("Header")

    //Corners
    writer.println("99.0,99.0,2001-01-01 00:00:00")
    writer.println("0.00001,0.0001,2001-01-01 00:00:00")
    writer.println("99.99,99.99,2001-01-01 00:00:00")
    writer.println("0.0001,99.99,2001-01-01 00:00:00")
    writer.println("99.9,0.0001,2001-01-01 00:00:00")
    writer.println("0.0,0.0,2001-01-01 00:00:00")

    //Not in range
    writer.println("0.0,100.0,2001-01-01 00:00:00")
    writer.println("100.0,0.0,2001-01-01 00:00:00")
    writer.println("100.0,100.0,2001-01-01 00:00:00")

    //In Range
    writer.println("10.0,10.0,2001-01-01 00:00:00")

    writer.flush()
    writer.close()
    val transformation = new Transformation()
    val tile = transformation.transformCSVtoRasterParametrised(settings, fileName, 0,1,2)
    assert(tile.toArrayDouble().reduce(_+_)==7)
  }

}
