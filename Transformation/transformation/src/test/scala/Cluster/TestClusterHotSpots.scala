package Cluster

import clustering.ClusterHotSpots
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntArrayTile, IntRawArrayTile}
import org.scalatest.FunSuite

/**
  * Created by marc on 11.05.17.
  */
class TestClusterHotSpots extends FunSuite{
  test("TestClusterHotSpots"){
    val chs = new ClusterHotSpots(getTestTile())
    println(chs.findClusters(1.5, 2)._1.asciiDraw())
    assert(chs.findClusters(1.5, 2)._2==9)

  }

  test("TestClusterHotSpots Big"){
    val testTile = Array.fill(500*500)(3)
    val rasterTile = new IntRawArrayTile(testTile, 500, 500)
    val chs = new ClusterHotSpots(rasterTile)
    assert(chs.findClusters(1.5, 2)._2==1)
  }



  test("Test replace Number"){
    val testTile = Array.fill(500*500)(3)
    val rasterTile = new IntRawArrayTile(testTile, 500, 500)
    val chs = new ClusterHotSpots(rasterTile)
    chs.replaceNumber(3,5,rasterTile)
  }

  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,0,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,0,0,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,3,
      0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,
      0,0,0,0,3,3,0,0,0,0,0,0,0,1,1,0,3,3,3,3,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,
      0,0,-3,-3,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,
      0,0,0,0,-3,0,0,1,0,0,0,0,0,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,0,0,3,0,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,3,0,0,0,0,3,3,0,0,0,0,0,0,
      0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,3,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,
      0,0,0,0,3,3,3,0,0,0,0,0,0,1,1,0,0,0,0,0,
      0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0)
    val weightTile = new DoubleRawArrayTile(arrayTile, 20,20)
    weightTile
  }
}
