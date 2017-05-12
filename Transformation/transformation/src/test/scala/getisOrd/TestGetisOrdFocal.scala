package getisOrd

import java.util.Random

import geotrellis.raster.{IntRawArrayTile, Tile}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by marc on 10.05.17.
  */
class TestGetisOrdFocal extends FunSuite with BeforeAndAfter {

  var getis : GetisOrdFocal = _
  var rasterTile : IntRawArrayTile = _

  before {
    val rnd = new Random(1)
    val testTile = Array.fill(100)(rnd.nextInt(100))
    rasterTile = new IntRawArrayTile(testTile, 10, 10)
    getis = new GetisOrdFocal(rasterTile, 10, 10, 2, Weight.One)
  }

  test("Test focal Mean 0,0"){
    val sum = (rasterTile.get(0,0)+rasterTile.get(0,1)+rasterTile.get(1,0)+rasterTile.get(1,1)+rasterTile.get(2,0)+rasterTile.get(0,2))
    assert(getis.getXMean(0,0)==sum/6)
  }

  test("Test focal Mean row,cols"){
    getis.setFocalRadius(1)
    val sum = (rasterTile.get(9,9)+rasterTile.get(8,9)+rasterTile.get(9,8))
    assert(getis.getXMean(9,9)==sum/3)
  }

  test("Test focal Mean"){
    getis.setFocalRadius(1)
    val sum = rasterTile.get(5,5)+rasterTile.get(5,6)+rasterTile.get(6,5)+rasterTile.get(4,5)+rasterTile.get(5,4)
    assert(getis.getXMean(5,5)==sum/5.0)
  }

  test("Test focal SD"){
    getis.setFocalRadius(1)
    val mean = getis.getXMean(5,5)
    val powerOfTile = Math.pow(rasterTile.get(5,5)-mean,2)
    +Math.pow(rasterTile.get(5,6)-mean,2)
    +Math.pow(rasterTile.get(6,5)-mean,2)
    +Math.pow(rasterTile.get(4,5)-mean,2)
    +Math.pow(rasterTile.get(5,4)-mean,2)


    val sdx5y5 = Math.sqrt(powerOfTile/5.0)
    assert(getis.getStandartDeviationForTile(5,5)==sdx5y5)
  }

}
