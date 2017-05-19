package getisOrd

import java.util.Random

import geotrellis.raster.mapalgebra.focal.Circle
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, IntRawArrayTile, Tile}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import parmeters.Settings

/**
  * Created by marc on 10.05.17.
  */
class TestGetisOrdFocal extends FunSuite with BeforeAndAfter {

  var getis : GetisOrdFocal = _
  var rasterTile : IntRawArrayTile = _
  var setting = new Settings()
  before {
    val rnd = new Random(1)
    val testTile = Array.fill(100)(rnd.nextInt(100))
    rasterTile = new IntRawArrayTile(testTile, 10, 10)
    setting.focalRange = 2
    getis = new GetisOrdFocal(rasterTile, setting)

    setting.weightMatrix = Weight.One
    getis.createNewWeight(setting)
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

  ignore("Test focal SD"){
    setting.focalRange = 1
    getis = new GetisOrdFocal(rasterTile, setting)
    val mean = getis.getXMean(5,5)
    val p1 = (rasterTile.getDouble(5,5),rasterTile.getDouble(5,6),rasterTile.getDouble(6,5),rasterTile.getDouble(4,5),rasterTile.getDouble(5,4))
    val powerOfTile = Math.pow(rasterTile.getDouble(5,5)-mean,2)
    +Math.pow(rasterTile.getDouble(5,6)-mean,2)
    +Math.pow(rasterTile.getDouble(6,5)-mean,2)
    +Math.pow(rasterTile.getDouble(4,5)-mean,2)
    +Math.pow(rasterTile.getDouble(5,4)-mean,2)


    val sdx5y5 = Math.sqrt(powerOfTile/5.0)
    assert(getis.getStandartDeviationForTile(5,5)==rasterTile.focalStandardDeviation(Circle(1)).getDouble(5,5))
    assert(getis.getStandartDeviationForTile(5,5)==sdx5y5)
  }

  test("Test other focal SD"){
    setting.focalRange = 1
    getis = new GetisOrdFocal(rasterTile, setting)
    val index = (5,5)
    val xMean = getis.getXMean(index)
    var sum = 0.0
    var size = 0
    for(i <- -setting.focalRange to setting.focalRange) {
      for (j <- -setting.focalRange to setting.focalRange) {
        if(Math.sqrt(i*i+j*j)<=setting.focalRange){
          val tmp = (rasterTile.getDouble(index._1+i, index._2+j))
          sum += Math.pow(tmp-xMean,2)
          size +=1
        }
      }
    }
    val sd = Math.sqrt(sum/size)

    assert(getis.getStandartDeviationForTile(5,5)==sd)
  }

  test("Test Indexing"){
    val row = 6
    val col = 7
    val range = 0 to (row)*(col)-1
    val r = range.map(index =>(index/row,index%row))

    for(i <- 0 to col-1){
      for(j <- 0 to row-1){
        assert(r.contains((i,j)))
      }
    }
    val conf = new SparkConf().setAppName("Test")
    conf.setMaster("local[1]")
    var spark : SparkContext = SparkContext.getOrCreate(conf)
    val tileG = IntArrayTile.ofDim(col, row)
    val result = spark.parallelize(range).map(index => (index/tileG.rows,index%tileG.rows,5)).collect()
    for(res <- result){
      tileG.set(res._1,res._2,res._3)
    }
    assert(tileG.get(0,0)==5)
    assert(tileG.get(col-1,row-1)==5)
  }

  test("Test Spark Focal G*"){
    setting.focalRange = 5
    val rnd = new Random(1)
    val testTile = Array.fill(1000000)(rnd.nextInt(100))
    rasterTile = new IntRawArrayTile(testTile, 1000, 1000)
    getis = new GetisOrdFocal(rasterTile, setting)
    println(getis.getSparkGstart(setting).getDouble(0,0))

  }

  test("Test local Focal G*"){
    setting.focalRange = 5
    val rnd = new Random(1)
    val testTile = Array.fill(100000)(rnd.nextInt(100))
    rasterTile = new IntRawArrayTile(testTile, 1000, 100)
    getis = new GetisOrdFocal(rasterTile, setting)
    println(getis.gStarComplete().getDouble(0,0))

  }

}
