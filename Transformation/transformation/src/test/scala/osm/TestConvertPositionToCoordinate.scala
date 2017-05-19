package osm

import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 16.05.17.
  */
class TestConvertPositionToCoordinate extends FunSuite {

  test("Test ConvertPositionToCoordinate bounds"){
    val cptc = new ConvertPositionToCoordinate()
    val para = new Settings()
    assert(cptc.getGPSCoordinate(0,0, para)==(para.latMin/para.multiToInt, (para.lonMin-para.shiftToPostive)/para.multiToInt))
    val maxResult = cptc.getGPSCoordinate(para.rasterLatLength,para.rasterLonLength, para)
    assert(maxResult._1>((para.latMax-para.sizeOfRasterLat.toDouble)/para.multiToInt) && maxResult._2 > ((para.lonMax-para.shiftToPostive-para.sizeOfRasterLon.toDouble)/para.multiToInt)
        && maxResult._1<((para.latMax+para.sizeOfRasterLat.toDouble)/para.multiToInt) && maxResult._2 < ((para.lonMax-para.shiftToPostive+para.sizeOfRasterLon.toDouble)/para.multiToInt))

  }

}
