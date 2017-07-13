package timeUtitls

import java.util.Random

import geotrellis.raster.{ArrayMultibandTile, ArrayTile, DoubleRawArrayTile, MultibandTile, Tile}
import org.scalatest.FunSuite
import timeUtils.MultibandUtils

/**
  * Created by marc on 12.07.17.
  */
object TestMultibandUtils extends FunSuite{
  var rnd = new Random(1)

  def getMultiband(f : => ArrayTile, bandsSize : Int): MultibandTile =  {
    val bands = new Array[Tile](bandsSize)
    for(i <- 0 to bandsSize-1){
      bands(i) = f
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    multiBand
  }

  def getMultiband(f : => ArrayTile): MultibandTile = {
    getMultiband(f,24)
  }

  def getMultibandTupleTileRandomWithoutReset(): (MultibandTile,MultibandTile) ={
    (getMultibandTileGeneric(24,100,100,nextInt),getMultibandTileGeneric(24,100,100,nextInt))
  }

  def getMultibandTileRandomWithoutReset(): MultibandTile ={
    getMultibandTileGeneric(24,100,100,nextInt)
  }

  def getMultibandTileRandom(): MultibandTile ={
    getMultibandTileGenericRandom(100,100)
  }

  def getMultibandTile1(): MultibandTile ={
    getMultibandTileGeneric(100,100,getOne)
  }

  def getMultibandTileGeneric1(rows : Int, cols : Int): MultibandTile ={
    getMultibandTileGeneric(24,rows,cols,getOne)
  }

  def getMultibandTileGenericRandom(rows : Int, cols : Int): MultibandTile ={
    rnd = new Random(1)
    getMultibandTileGeneric(24,rows,cols,nextInt)
  }

  def getMultibandTileGeneric(rows : Int, cols : Int, f : () => Double): MultibandTile ={
    getMultibandTileGeneric(24,rows,cols,getOne)
  }

  def getMultibandTileGeneric(bands : Int, rows : Int, cols : Int, f : () => Double): MultibandTile ={
    getMultiband(newArrayTile(cols,rows,f))
  }

  def getOne() : Double = {
    1.0
  }

  def nextInt() : Double = {
    rnd.nextInt(100)
  }

  def newArrayTile(cols : Int, rows : Int, f :() => Double) : ArrayTile = {
    new DoubleRawArrayTile(Array.fill(cols*rows)(f.apply()), cols, rows)
  }


  test("isInTile") {
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(10000)(rnd.nextInt(100))
    val rasterTile1 : Tile = new DoubleRawArrayTile(testTile, 100, 100)
    val testTile2 : Array[Double]= Array.fill(10000)(rnd.nextInt(100))
    val rasterTile2 : Tile = new DoubleRawArrayTile(testTile2, 100, 100)
    val bands = Array(rasterTile1,rasterTile2)
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    assert(MultibandUtils.isInTile(101,100, multiBand)==false)
    assert(MultibandUtils.isInTile(100,100, multiBand)==false)
    assert(MultibandUtils.isInTile(100,100, multiBand)==false)
    assert(MultibandUtils.isInTile(-101,-100, multiBand)==false)
    assert(MultibandUtils.isInTile(101,-100, multiBand)==false)
    assert(MultibandUtils.isInTile(100,50, multiBand)==false)
    assert(MultibandUtils.isInTile(50,100, multiBand)==false)

    assert(MultibandUtils.isInTile(50,50, multiBand)==true)
    assert(MultibandUtils.isInTile(99,99, multiBand)==true)
  }

}
