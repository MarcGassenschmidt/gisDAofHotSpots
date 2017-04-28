package getisOrt


import java.util.Random

import geotrellis.raster.IntArrayTile
import gisOrt.GetisOrt
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by marc on 28.04.17.
  */
class TestGetisOrt extends FunSuite {

  test("Test GetisOrt Implementation"){
    val gStart = new GetisOrt()
    val weightTile = IntArrayTile.fromBytes(gStart.getWeightMatrix(), 3,3)
    val rnd = new Random()
    val testTile = Array.fill(1200)(1.toByte)
    val rasterTile = IntArrayTile fromBytes(testTile, 300, 400)
    println(gStart.gStarForTile(rasterTile, (150,200), weightTile))
  }



}
