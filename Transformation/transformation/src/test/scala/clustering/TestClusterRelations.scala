package clustering

import clustering.ClusterRelations
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntRawArrayTile}
import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by marc on 12.05.17.
  */
class TestClusterRelations extends FunSuite{
  test("Test ClusterRelations::rescaleBiggerTile"){
    val cr = new ClusterRelations()
    var tile1 = getTile(4,5)
    var tile2 = getTile(4,5)
    cr.rescaleBiggerTile(tile1,tile2)
    assert(tile1.cols==tile2.cols)
    assert(tile1.rows==tile2.rows)
    tile1 = getTile(2,2)
    tile2 = getTile(4,5)
    println(tile1.asciiDrawDouble())
    println(tile2.asciiDrawDouble())
    println(tile1.resample(1,1).asciiDrawDouble())

    var result = cr.rescaleBiggerTile(tile1,tile2)
    println(result._1.asciiDrawDouble())
    println(result._2.asciiDrawDouble())

    assert(result._1.cols==7)
    assert(7==result._2.cols)
    assert(result._1.rows==7)
    assert(7==result._2.rows)
    tile1 = getTile(7,7)
    tile2 = getTile(8,8)
    result = cr.rescaleBiggerTile(tile1,tile2)
    assert(result._1.cols==8)
    assert(8==result._2.cols)
    assert(result._1.rows==8)
    assert(8==result._2.rows)
    tile1 = getTile(7,7)
    tile2 = getTile(4,8)
    result = cr.rescaleBiggerTile(tile1,tile2)
    assert(result._1.cols==7)
    assert(7==result._2.cols)
    assert(result._1.rows==8)
    assert(8==result._2.rows)
    tile1 = getTile(7,7)
    tile2 = getTile(9,5)
    result = cr.rescaleBiggerTile(tile1,tile2)
    assert(result._1.cols==9)
    assert(9==result._2.cols)
    assert(result._1.rows==7)
    assert(7==result._2.rows)
    tile1 = getTile(7,4)
    tile2 = getTile(4,5)
    result = cr.rescaleBiggerTile(tile1,tile2)
    assert(result._1.cols==7)
    assert(7==result._2.cols)
    assert(result._1.rows==5)
    assert(5==result._2.rows)
    assert(result._1.findMinMax==(1,1))
    assert((1,1)==result._2.findMinMax)
  }


  def getTile(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(1)//new Random().nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }

  test("Test ClusterRelations::getNumberChildrenAndParentsWhichIntersect"){
    val cr = new ClusterRelations()
    assert((getTestClusterChild()-getTestClusterParent()).toArrayDouble().filter(x => x>0).distinct.length==3)
    assert(cr.getNumberChildrenAndParentsWhichIntersect(getTestClusterParent(),getTestClusterChild())==(3,7))
  }

  def getTestClusterChild(): ArrayTile ={
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
