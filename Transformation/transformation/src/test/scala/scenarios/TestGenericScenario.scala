package scenarios

import geotrellis.raster.{ArrayTile, IntRawArrayTile, MultibandTile}
import org.scalatest.FunSuite
import parmeters.Settings

import scala.util.Random


/**
  * Created by marc on 08.06.17.
  */
class TestGenericScenario extends FunSuite{

  ignore("test aggregate zoom"){
    val gs =new GenericScenario()
    val tile1 = getTile(8,8)
    var result = gs.aggregateToZoom(tile1,8)
    assert(result.getDouble(0,0)==tile1.toArrayDouble().reduce(_+_))
    result = gs.aggregateToZoom(tile1,2)
    assert(result.cols==4)
    result = gs.aggregateToZoom(tile1,3)
    assert(result.cols==2)
  }


  ignore("test aggregate"){
    val gs =new GenericScenario()
    val tile1 = getTile(2,2)
    val result = gs.aggregateTile(tile1)
    assert(result.getDouble(0,0)==tile1.toArrayDouble().reduce(_+_))
  }

  def getTile(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(new Random().nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }

  test("Raster From Geo Tiff"){
    val gs = new GenericScenario()
    val settings = new Settings()
    settings.fromFile = true
    val tile = gs.getRasterFromGeoTiff(settings, 0, 0,0,"test/raster", getTile(100,100))
    println(tile.asciiDrawDouble())
  }

  test("Raster From MulitGeo Tiff"){
    val gs = new GenericScenario()
    val settings = new Settings()
    settings.fromFile = true
    val tile = gs.getRasterFromMulitGeoTiff(settings, 0, 0,0,"test/raster", MultibandTile(Array(getTile(100,100),getTile(100,100))))
    println(tile.band(0).asciiDrawDouble())
  }

}
