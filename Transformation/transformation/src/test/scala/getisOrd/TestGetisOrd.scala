package getisOrd


import java.util.Random

import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntArrayTile, IntRawArrayTile}
import org.scalatest.{BeforeAndAfter, FunSuite}
import parmeters.Settings

/**
  * Created by marc on 28.04.17.
  */
class TestGetisOrd extends FunSuite {

  val set = new Settings()

  test("Test getSquareWeight"){
    val testTile = Array.fill(120000)(2)
    val rasterTile = new IntRawArrayTile(testTile, 300, 400)


    val gStart = new GetisOrd(rasterTile, set)
    assert(gStart.getWeightMatrixSquare(3).equals(getTestTile()))
  }

  test("Test w2sum"){
    val testTile: Array[Double] = Array.fill(1200)(2.0)
    val rasterTile = new DoubleRawArrayTile(testTile, 30, 40)
    val gStart = new GetisOrd(rasterTile, set)
    assert(4.0*1200==gStart.getPowerOfTwoForElementsAsSum(rasterTile))
    assert(1200*2.0==gStart.getSummForTile(rasterTile))
    assert(0==(1200*gStart.getPowerOfTwoForElementsAsSum(rasterTile)-gStart.getSummForTile(rasterTile)*gStart.getSummForTile(rasterTile))/(1200-1))
  }

  ignore("Test ND"){
    val testTile: Array[Double] = Array.fill(1200)(Double.NaN)
    val rasterTile = new DoubleRawArrayTile(testTile, 30, 40)
    println(rasterTile.asciiDrawDouble())
    println(rasterTile.mapIfSet(x=> x).asciiDrawDouble())

  }

  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,1,0,0,0,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      1,1,1,1,1,1,1,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      0,0,0,1,0,0,0
      )
    val weightTile = new DoubleRawArrayTile(arrayTile, 7,7)
    weightTile
  }


  ignore("Test GetisOrt Implementation"){
    val testTile = Array.fill(120000)(2)
    val rasterTile = new IntRawArrayTile(testTile, 300, 400)
    val gStart = new GetisOrd(rasterTile, set)
    assert(gStart.gStarForTile(150,200) >= (-8.517 - 0.01) || gStart.gStarForTile(150,200) <= (-8.517 + 0.01))
  }

  test("Test GetisOrt with Random"){
    val rnd = new Random(1)
    val testTile = Array.fill(100)(rnd.nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, 10, 10)
    val gStart = new GetisOrd(rasterTile,set)
    println(rasterTile.asciiDraw())
    println("Weight ="+gStart.weight.asciiDraw())

    println("Sum of Weight="+gStart.sumOfWeight)
    println("X-Mean="+gStart.xMean)
    println("Power of Weight element wise="+gStart.powerOfWeight)
    println("Power of Tile element wise="+gStart.powerOfTile)
    println("Standard Devivation="+gStart.getStandartDeviationForTile(rasterTile))
    println("Denominator="+gStart.getDenominator())
    println(gStart.printG_StarComplete())
  }

  ignore("Test Weight"){
    val rnd = new Random(1)
    val testTile = Array.fill(100)(rnd.nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, 10, 10)
    val gStart = new GetisOrd(rasterTile,set)
    println(gStart.getWeightMatrixDefined(4,4).toArrayDouble().mkString(":"))

  }



}
