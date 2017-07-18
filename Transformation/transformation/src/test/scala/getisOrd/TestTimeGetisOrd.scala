package getisOrd

import java.util.Random

import geotrellis.Spheroid
import geotrellis.raster.{ArrayMultibandTile, ArrayTile, DoubleCellType, DoubleRawArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector.{Extent, Line, Point, Polygon}
import importExport.{ImportGeoTiff, PathFormatter}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import parmeters.Settings
import timeUtils.MultibandUtils

import scala.collection.mutable

/**
  * Created by marc on 03.07.17.
  */
class TestTimeGetisOrd extends FunSuite {

  test("getMultibandFocalGetisOrd") {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    var myKey = new SpatialKey(0,0)
    val result = TimeGetisOrd.getMultibandFocalGetisOrd(multiBand,ownSettings, myKey,hashMap)
    assert(result.bandCount==multiBand.bandCount)
    assert(result.rows==multiBand.rows)
    assert(result.band(0).getDouble(0,0)!=multiBand.band(0).getDouble(0,0))
  }

  test("test getSd"){
    val getSetupResult: (Spheroid, MultibandTile, mutable.HashMap[SpatialKey, MultibandTile], SpatialKey) = getSetup
    val spheroid: Spheroid = getSetupResult._1
    val multiBand: MultibandTile = getSetupResult._2
    var hashMap: mutable.HashMap[SpatialKey, MultibandTile] = getSetupResult._3
    var myKey: SpatialKey = getSetupResult._4

    var mean = 1
    val r = TimeGetisOrd.getSD(0,10,10, multiBand, spheroid, myKey, hashMap, 1)


    assert(r == Math.sqrt((1.0/(spheroid.getSum().toDouble-1.0))*spheroid.getSum().toDouble))
  }

  def getSetup: (Spheroid, MultibandTile, mutable.HashMap[SpatialKey, MultibandTile], SpatialKey) = {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    val spheroid = new Spheroid(2, 1)
    for (i <- 0 to 23) {
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(2), 100, 100)
    }
    val multiBand: MultibandTile = new ArrayMultibandTile(bands)
    var hashMap = new mutable.HashMap[SpatialKey, MultibandTile]()
    var myKey = new SpatialKey(0, 0)
    (spheroid, multiBand, hashMap, myKey)
  }

  test("test getNM"){
    val getSetupResult: (Spheroid, MultibandTile, mutable.HashMap[SpatialKey, MultibandTile], SpatialKey) = getSetup
    val spheroid: Spheroid = getSetupResult._1
    val multiBand: MultibandTile = getSetupResult._2
    var hashMap: mutable.HashMap[SpatialKey, MultibandTile] = getSetupResult._3
    var myKey: SpatialKey = getSetupResult._4

    val r = TimeGetisOrd.getNM(0,10,10, multiBand, spheroid, myKey, hashMap)
    assert(r._1==spheroid.getSum())
    assert(r._2==2)
  }

  test("getStatsFocal"){
    val getSetupResult: (Spheroid, MultibandTile, mutable.HashMap[SpatialKey, MultibandTile], SpatialKey) = getSetup
    val spheroidWeight: Spheroid = getSetupResult._1
//    val spheroidFocal: Spheroid = new Spheroid(spheroidWeight.a+2,spheroidWeight.c+1)
    val multiBand: MultibandTile = getSetupResult._2
    var hashMap: mutable.HashMap[SpatialKey, MultibandTile] = getSetupResult._3
    var myKey: SpatialKey = getSetupResult._4

    val corner = TimeGetisOrd.getRMWNW2(0,0,0,multiBand, spheroidWeight,myKey, hashMap, (10,1))
    val normal = TimeGetisOrd.getRMWNW2(0,10,10,multiBand, spheroidWeight,myKey, hashMap, (10,1))
    assert(corner.equals(new StatsRNMW(2*8,10,1,8,8*10,8*8)))
    assert(normal.equals(new StatsRNMW(spheroidWeight.getSum()*2,10,1,spheroidWeight.getSum(),10*spheroidWeight.getSum(),Math.pow(spheroidWeight.getSum(),2))))
  }

  test("getMultibandGetisOrd") {
    val ownSettings = new Settings()
    ownSettings.focalRange = 5
    ownSettings.layoutTileSize = (100,100)
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    var myKey = new SpatialKey(0,0)
    val result = TimeGetisOrd.getMultibandGetisOrd(multiBand,ownSettings, TimeGetisOrd.getSTGlobal(multiBand), myKey,hashMap)
    assert(result.bandCount==multiBand.bandCount)
    assert(result.rows==multiBand.rows)
    assert(result.band(0).getDouble(0,0)!=multiBand.band(0).getDouble(0,0))
  }

  test("getSum") {
    val spheroid = new Spheroid(2,1)
    val bands = new Array[Tile](24)

    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(1.0), 100, 100)
    }


    val multiBand : MultibandTile = new ArrayMultibandTile(bands)

    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()

    var myKey = new SpatialKey(0,0)
    val result = TimeGetisOrd.getSum(multiBand,spheroid, myKey,hashMap)

    val corner = 8
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
    val corner = 8
    val normal = spheroid.getSum()
    var hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    var myKey = new SpatialKey(0,0)
    assert(TimeGetisOrd.getSum(0,50,50,multiBand,spheroid, myKey, hashMap)==normal)
    assert(TimeGetisOrd.getSum(0,0,0,multiBand,spheroid, myKey,hashMap)==corner)

    assert(TimeGetisOrd.getSum(1,50,50,multiBand,spheroid, myKey,hashMap)==normal)
    assert(TimeGetisOrd.getSum(1,0,0,multiBand,spheroid, myKey,hashMap)==corner)

    assert(TimeGetisOrd.getSum(3,50,50,multiBand,spheroid,myKey, hashMap)==normal)
    assert(TimeGetisOrd.getSum(3,0,0,multiBand,spheroid,myKey, hashMap)==corner)

    myKey = new SpatialKey(0,0)

    hashMap.put(new SpatialKey(1,1), multiBand)
    hashMap.put(new SpatialKey(0,1), multiBand)
    hashMap.put(new SpatialKey(1,0), multiBand)


    assert(TimeGetisOrd.getSum(0,50,50,multiBand,spheroid,myKey, hashMap)==normal)
    assert(TimeGetisOrd.getSum(0,0,0,multiBand,spheroid,myKey, hashMap)==corner)
    assert(TimeGetisOrd.getSum(0,99,99,multiBand,spheroid, myKey,hashMap)==normal)


    hashMap  = new mutable.HashMap[SpatialKey,MultibandTile]()
    myKey = new SpatialKey(1,1)
    hashMap.put(new SpatialKey(0,0), multiBand)
    hashMap.put(new SpatialKey(0,1), multiBand)
    hashMap.put(new SpatialKey(0,2), multiBand)
    hashMap.put(new SpatialKey(1,0), multiBand)
    hashMap.put(new SpatialKey(2,0), multiBand)
    hashMap.put(new SpatialKey(1,2), multiBand)
    hashMap.put(new SpatialKey(2,1), multiBand)
    hashMap.put(new SpatialKey(2,2), multiBand)

    assert(TimeGetisOrd.getSum(0,50,50,multiBand,spheroid,myKey, hashMap)==normal)
    assert(TimeGetisOrd.getSum(0,0,0,multiBand,spheroid, myKey,hashMap)==normal)


  }

  test("getGetisOrd") {
    val importTer = new ImportGeoTiff()
    val setting = new Settings
    setting.focal = false
    setting.test = true
    setting.layoutTileSize = (50,30)
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(90*100)(rnd.nextInt(100)), 100, 90)
    }

    var multiBand : MultibandTile = new ArrayMultibandTile(bands)
    importTer.writeMultiGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    var rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    var result = TimeGetisOrd.getGetisOrd(rdd,setting, multiBand)
    setting.focal = true
    setting.layoutTileSize = (5,6)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10*12)(rnd.nextInt(100)), 10, 12)
    }
    multiBand = new ArrayMultibandTile(bands)
    importTer.writeMultiGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    result = TimeGetisOrd.getGetisOrd(rdd,setting, multiBand)

  }



  test("polygonalSumDouble test"){
    //Histogramm is not working
    val bands = new Array[Tile](24)
    val rnd = new Random(1)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    multiBand.bands.map(x=>(x.polygonalSumDouble(new Extent(0,0,100,100), (new Extent(0,0,100,100)).toPolygon()),x.toArrayDouble().reduce(_+_))).foreach(x=>assert(x._1==x._2))
  }

  test("filterNoData"){
    val bands = new Array[Tile](24)
    val rnd = new Random(1)
    for(i <- 0 to 23){
      if(i%2==1){
        bands(i) = new DoubleRawArrayTile(Array.fill(100)(if(rnd.nextInt(100)>50) 1 else Double.NegativeInfinity), 10, 10)
      } else {
        bands(i) = new DoubleRawArrayTile(Array.fill(100)(if(rnd.nextInt(100)>50) 1 else Double.NaN), 10, 10)
      }

    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    multiBand.bands.map(x=>x.toArrayDouble().filter(f=>TimeGetisOrd.isNotNaN(f))).foreach(x=>x.foreach(y=>assert(y>0)))
  }

  test("stats global"){
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    val st = TimeGetisOrd.getSTGlobal(multiBand)
    assert(st.gN == 24*100*100)
    assert(st.gM == multiBand.bands.map(x=>x.toArrayDouble().reduce(_+_)).reduce(_+_)/st.gN)
    assert(st.gS > 0)
  }

  test("lookup"){
    val importTer = new ImportGeoTiff()
    val setting = new Settings
    setting.focal = false
    setting.test = true
    val rnd = new Random(1)
    val bands = new Array[Tile](24)
    for(i <- 0 to 23){
      bands(i) = new DoubleRawArrayTile(Array.fill(10000)(rnd.nextInt(100)), 100, 100)
    }
    setting.layoutTileSize = (10,10)
    val multiBand : MultibandTile = new ArrayMultibandTile(bands)
    importTer.writeMultiGeoTiff(multiBand, setting, "/tmp/firstTimeBand.tif")
    val rdd = importTer.repartitionFiles("/tmp/firstTimeBand.tif", setting)
    val broadcast = SparkContext.getOrCreate(setting.conf).broadcast(rdd.collect())
    var r = TimeGetisOrd.getNeigbours(new SpatialKey(0,0), broadcast)
    assert(r.size==3)
    assert(r.contains(new SpatialKey(1,0)))
    assert(r.contains(new SpatialKey(0,1)))
    assert(r.contains(new SpatialKey(1,1)))

    r = TimeGetisOrd.getNeigbours(new SpatialKey(1,1), broadcast)
    assert(r.size==8)
  }



}
