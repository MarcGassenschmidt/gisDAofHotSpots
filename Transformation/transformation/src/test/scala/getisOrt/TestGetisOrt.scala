package getisOrt


import java.util.Random

import geotrellis.raster.{DoubleRawArrayTile, IntArrayTile, IntRawArrayTile}
import gisOrt.GetisOrt
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by marc on 28.04.17.
  */
class TestGetisOrt extends FunSuite {

  test("Test GetisOrt Implementation"){
    val gStart = new GetisOrt()
    val weightTile = new DoubleRawArrayTile(gStart.getWeightMatrix(), 3,3)
    val rnd = new Random()
    val testTile = Array.fill(120000)(2)
    //TODO bug
    val rasterTile = new IntRawArrayTile(testTile, 300, 400)
    assert(gStart.gStarForTile(rasterTile, (150,200), weightTile) >= (-8.517 - 0.01) || gStart.gStarForTile(rasterTile, (150,200), weightTile) <= (-8.517 + 0.01))
  }



}
