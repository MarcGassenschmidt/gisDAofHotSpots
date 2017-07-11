package getisOrd

import geotrellis.raster.{ArrayTile, DoubleRawArrayTile}
import org.scalatest.FunSuite

/**
  * Created by marc on 07.06.17.
  */
class TestSoH extends FunSuite{

  test("Test validation of SoH"){

    var sohValuesGood = SoH.getSoHDowAndUp(getTestClusterParent(),getTestClusterChild1())
    println(sohValuesGood)
    var sohValuesBad = SoH.getSoHDowAndUp(getTestClusterParent(),getTestClusterChild2())
    println(sohValuesBad)
    assert(sohValuesGood._1>sohValuesBad._1 && sohValuesGood._2>sohValuesBad._2)
    var sohValues = SoH.getSoHDowAndUp(getSmallParent(),getSmallChild1())
    println(sohValues)
    sohValues = SoH.getSoHDowAndUp(getSmallParent(),getSmallChild2())
    println(sohValues)
    assert(SoH.getSoHDowAndUp(getParent1(),getChild1)==(1,1))
    assert(SoH.getSoHDowAndUp(getParent1(),getChild2)==(1,0))
    assert(SoH.getSoHDowAndUp(getParent1(),getChild3)==(1,1))
    assert(SoH.getSoHDowAndUp(getParent2(),getChild4)==(1,0))
    assert(SoH.getSoHDowAndUp(getParent2(),getChild1)==(0,0))
    assert(SoH.getSoHDowAndUp(getParent2(),getChild5)==(0.5,1))


  }

  def getParent1(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,1,1,0,
      0,0,1,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getParent2(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,0,2,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getChild1(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,0,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getChild2(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,1,1,
      0,0,0,1,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getChild3(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,0,2,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getChild4(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,1,1,0,
      0,0,1,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getChild5(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,0,0,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 2,5)
    weightTile
  }

  def getSmallChild2(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,1,1,
      0,0,0,1,0,
      0,0,0,0,0,
      0,0,0,0,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getSmallChild1(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,0,0,
      0,0,0,0,0,
      0,0,0,0,0,
      0,0,0,0,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getSmallParent(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,1,1,0,
      0,0,1,0,0,
      0,0,0,0,0,
      0,0,0,0,0,
      0,0,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }


  def getTestClusterChild1(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,
      0,0,2,2,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,
      0,0,0,0,2,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,5,5,0,0,0,0,0,0,0,4,4,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,4,4,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,8,8,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,8,8,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }

  def getTestClusterChild2(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,5,5,5,5,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,5,5,5,0,0,0,0,0,0,4,4,0,3,3,3,3,
      0,0,0,0,0,5,5,0,0,0,0,0,0,4,4,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,8,8,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,8,8,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }

  def getTestClusterParent(): ArrayTile ={
    val arrayTile = Array[Double](
      0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,2,2,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,
      0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,8,8,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,8,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,4,4,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,4,4,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,7,7,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }
}
