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
    val testTile = Array.fill(120000)(2)
    val rasterTile = new IntRawArrayTile(testTile, 300, 400)
    val gStart = new GetisOrt(rasterTile, 0,0)
    assert(gStart.gStarForTile(150,200) >= (-8.517 - 0.01) || gStart.gStarForTile(150,200) <= (-8.517 + 0.01))
  }

  test("Test GetisOrt with Random"){
    val rnd = new Random(1)
    val testTile = Array.fill(100)(rnd.nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, 10, 10)
    val gStart = new GetisOrt(rasterTile,3,3)
    println(rasterTile.asciiDraw())
    println("Weight ="+gStart.weight.asciiDraw())
    println("Sum of Tile="+gStart.sumOfTile)
    println("Sum of Weight="+gStart.sumOfWeight)
    println("X-Mean="+gStart.xMean)
    println("Power of Weight element wise="+gStart.powerOfWeight)
    println("Power of Tile element wise="+gStart.powerOfTile)
    println("Standard Devivation="+gStart.standardDeviation)
    println("Denominator="+gStart.getDenominator())
    println(gStart.printG_StarComplete())
  }



}
