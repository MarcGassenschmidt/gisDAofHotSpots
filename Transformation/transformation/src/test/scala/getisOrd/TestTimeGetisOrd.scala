package getisOrd

import java.util.Random

import geotrellis.Spheroid
import geotrellis.raster.{ArrayMultibandTile, DoubleRawArrayTile, MultibandTile, Tile}
import importExport.ImportGeoTiff
import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 03.07.17.
  */
class TestTimeGetisOrd extends FunSuite {

  ignore("getMultibandFocalGetisOrd") {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val result = TimeGetisOrd.getMultibandFocalGetisOrd(multiBand,ownSettings)
    assert(result.bandCount==multiBand.bandCount)
    assert(result.rows==multiBand.rows)
    assert(result.band(0).getDouble(0,0)!=multiBand.band(0).getDouble(0,0))
  }

  test("getMultibandGetisOrd") {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val result = TimeGetisOrd.getMultibandGetisOrd(multiBand,ownSettings)
    assert(result.bandCount==multiBand.bandCount)
    assert(result.rows==multiBand.rows)
    assert(result.band(0).getDouble(0,0)!=multiBand.band(0).getDouble(0,0))
  }

  test("isInTile") {
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(10000)(rnd.nextInt(100))
    val rasterTile1 : Tile = new DoubleRawArrayTile(testTile, 100, 100)
    val testTile2 : Array[Double]= Array.fill(10000)(rnd.nextInt(100))
    val rasterTile2 : Tile = new DoubleRawArrayTile(testTile2, 100, 100)
    val bands = Array(rasterTile1,rasterTile2)
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    assert(TimeGetisOrd.isInTile(101,100, multiBand)==false)
    assert(TimeGetisOrd.isInTile(100,100, multiBand)==false)
    assert(TimeGetisOrd.isInTile(100,100, multiBand)==false)
    assert(TimeGetisOrd.isInTile(-101,-100, multiBand)==false)
    assert(TimeGetisOrd.isInTile(101,-100, multiBand)==false)
    assert(TimeGetisOrd.isInTile(100,50, multiBand)==false)
    assert(TimeGetisOrd.isInTile(50,100, multiBand)==false)

    assert(TimeGetisOrd.isInTile(50,50, multiBand)==true)
    assert(TimeGetisOrd.isInTile(99,99, multiBand)==true)
  }

  test("getSum") {
    val spheroid = new Spheroid(2,1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(1.0), 100, 100)
    }

    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val result = TimeGetisOrd.getSum(multiBand,spheroid)

    val corner = 5
    val normal = spheroid.getSum()

    assert(result.band(0).getDouble(50,50)==normal)
    assert(result.band(0).getDouble(0,0)==corner)

    assert(result.band(23).getDouble(50,50)==normal)
    assert(result.band(23).getDouble(0,0)==corner)

    assert(result.band(12).getDouble(50,50)==normal)
    assert(result.band(12).getDouble(0,0)==corner)

  }
  test("getSum with coordinates") {
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(1.0), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val spheroid = new Spheroid(2,1)
    val corner = 5
    val normal = spheroid.getSum()

    assert(TimeGetisOrd.getSum(0,50,50,multiBand,spheroid)==normal)
    assert(TimeGetisOrd.getSum(0,0,0,multiBand,spheroid)==corner)

    assert(TimeGetisOrd.getSum(1,50,50,multiBand,spheroid)==normal)
    assert(TimeGetisOrd.getSum(1,0,0,multiBand,spheroid)==corner)

    assert(TimeGetisOrd.getSum(3,50,50,multiBand,spheroid)==normal)
    assert(TimeGetisOrd.getSum(3,0,0,multiBand,spheroid)==corner)
  }

  test("getGetisOrd") {
    val importTer = new ImportGeoTiff()
    val setting = new Settings
    setting.focal = false
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    importTer.writeMulitGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    val rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    var result = TimeGetisOrd.getGetisOrd(rdd,setting)
    setting.focal = true
    result = TimeGetisOrd.getGetisOrd(rdd,setting)
    //TODO
  }


}
