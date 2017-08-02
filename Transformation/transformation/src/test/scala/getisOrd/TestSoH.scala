package getisOrd

import clustering.{ClusterHotSpots, ClusterHotSpotsTime}
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, MultibandTile}
import geotrellis.spark.SpatialKey
import org.scalatest.FunSuite
import parmeters.Settings
import timeUtils.MultibandUtils
import timeUtitls.TestMultibandUtils

import scala.collection.mutable

/**
  * Created by marc on 07.06.17.
  */
class TestSoH extends FunSuite{

  test("getSDForPercentualTiles"){
    val settings = new Settings
    settings.focalRange = 1
    settings.focalRangeTime = 1
    val badExample = SoH.getSDForPercentualTiles(TestMultibandUtils.getMultiband(getParent2),settings)
    settings.focalRange = 20
    settings.focalRangeTime = 1
    val goodExample = SoH.getSDForPercentualTiles(TestMultibandUtils.getMultibandTile1(),settings)
    assert(goodExample<badExample) //Small value is better
  }

  test("Distance"){
    val distance = SoH.getDistance(TestMultibandUtils.getMultiband(TestMultibandUtils.getTestTileCluster(),24))
    println(distance)
    assert(distance>0 && distance<1)

  }


  test("MoransI"){
    assert(SoH.getMoransI(TestMultibandUtils.getCheesBoardRaster())> -1)
  }

  test("F1 Score"){
    val f1score = SoH.getF1Score(TestMultibandUtils.getMultiband(TestMultibandUtils.getTestTileCluster()),TestMultibandUtils.getMultiband(TestMultibandUtils.getTestTileCluster2()))
    //TODO Validate result
    assert(f1score==8.746430756934297)
  }


  ignore("Metrik Result"){
    val setting = new Settings
    var hashMap = new mutable.HashMap[SpatialKey, MultibandTile]()
    setting.layoutTileSize = (100,100)
    var mbT = TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandom(),setting, new SpatialKey(0,0),hashMap)
    var mbTCluster = (new ClusterHotSpotsTime(mbT)).findClusters()
    setting.weightRadius += 1
    var weightP = (new ClusterHotSpotsTime((TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap)))).findClusters()
    setting.weightRadius -= 2
    var weightN = (new ClusterHotSpotsTime(TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap))).findClusters()
    setting.weightRadius += 1
    var weightPN = (weightP,weightN)
    var focalPN = (new ClusterHotSpotsTime(TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap)).findClusters(),
                  (new ClusterHotSpotsTime(TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap))).findClusters())
    var aggreagtePN = (new ClusterHotSpotsTime(TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset.resample(50,50),setting, new SpatialKey(0,0),hashMap)).findClusters(),
                      (new ClusterHotSpotsTime(TimeGetisOrd.getMultibandFocalGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset.resample(200,200),setting, new SpatialKey(0,0),hashMap))).findClusters())
    var month = (new ClusterHotSpots((new GetisOrdFocal(TestMultibandUtils.newArrayTile(100,100,TestMultibandUtils.nextInt), setting).gStarComplete()))).findClusters(1.9,5)._1

    var focalResults = SoH.getMetrikResults(mbT,mbTCluster,aggreagtePN,weightPN,focalPN,month,setting,weightP)

    println(focalResults.toString)

    mbT = TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandom(), setting, new SpatialKey(0,0),hashMap)
    mbTCluster = (new ClusterHotSpotsTime(mbT)).findClusters()
    setting.weightRadius += 1
    weightP = (TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap))
    setting.weightRadius -= 2
    weightN = TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap)
    setting.weightRadius += 1
    weightPN = (weightP,weightN)
    focalPN = (TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap),
      TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset,setting, new SpatialKey(0,0),hashMap))

    setting.layoutTileSize = (50,50)
    val aP = TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset.resample(50,50),setting, new SpatialKey(0,0),hashMap)
    setting.layoutTileSize = (200,200)
    val aN  = TimeGetisOrd.getMultibandGetisOrd(TestMultibandUtils.getMultibandTileRandomWithoutReset.resample(200,200),setting, new SpatialKey(0,0),hashMap)
    aggreagtePN = (aP,aN)
    month = (new ClusterHotSpots((new GetisOrdFocal(TestMultibandUtils.newArrayTile(100,100,TestMultibandUtils.nextInt), setting).gStarComplete()))).findClusters(1.9,5)._1

    var globalResults = SoH.getMetrikResults(mbT,mbTCluster,aggreagtePN,weightPN,focalPN,month,setting,weightP)

    println(globalResults.toString)
  }



  test("KL"){
    val random = SoH.getKL(TestMultibandUtils.getMultibandTileRandom(),TestMultibandUtils.getMultibandTileRandomWithoutReset())
    assert(random<1)
    val randomSame = SoH.getKL(TestMultibandUtils.getMultibandTileRandom(),TestMultibandUtils.getMultibandTileRandom())
    assert(randomSame==0)
    val one = SoH.getKL(TestMultibandUtils.getMultibandTile1(),TestMultibandUtils.getMultibandTile1())
    assert(one==0)
  }

  test("Test SoH neighbours"){
    val focal = SoH.getSoHNeighbours(TestMultibandUtils.getMultibandTileRandom(),
      TestMultibandUtils.getMultibandTupleTileRandomWithoutReset(),
      TestMultibandUtils.getMultibandTupleTileRandomWithoutReset(),
      TestMultibandUtils.getMultibandTupleTileRandomWithoutReset()
    )
    assert(focal==false)
    val global = SoH.getSoHNeighbours(TestMultibandUtils.getMultibandTileRandom(),
      TestMultibandUtils.getMultibandTupleTileRandomWithoutReset(),
      TestMultibandUtils.getMultibandTupleTileRandomWithoutReset()
    )
    assert(global==false)
  }

  test("Test isStable"){
    //For true,false look at SoH Test
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild1),SoH.Neighbours.Aggregation)==true)
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild2),SoH.Neighbours.Aggregation)==false)
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild3),SoH.Neighbours.Aggregation)==true)
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild4),SoH.Neighbours.Aggregation)==false)
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild1),SoH.Neighbours.Aggregation)==false)
    assert(SoH.isStable(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild5),SoH.Neighbours.Aggregation)==true)
  }

  test("compare with Tile"){
    val testWihtTile = SoH.compareWithTile(TestMultibandUtils.getMultibandTileRandom(),TestMultibandUtils.newArrayTile(100,100,TestMultibandUtils.nextInt))
    assert(testWihtTile==(1.0,0.37878787878787884))
  }

  ignore("Test cdf"){
    var histogramm = MultibandUtils.getHistogramInt(TestMultibandUtils.getMultibandTileRandom())
    //histogramm.cdf().map(x=>println(x._1+","+x._2))
    histogramm = MultibandUtils.getHistogramInt(TestMultibandUtils.getMultiband(getParent2))
    //histogramm.cdf().map(x=>println(x._1+","+x._2))
    histogramm = MultibandUtils.getHistogramInt(TestMultibandUtils.getMultibandTile1())
    histogramm.cdf().map(x=>println(x._1+","+x._2))
  }

  test("Jaccard index"){
    var parent = TestMultibandUtils.getMultiband(getParent2())
    var child = TestMultibandUtils.getMultiband(getChild2())
    assert(SoH.getJaccardIndex(parent,child)==1/2.0)
    parent = TestMultibandUtils.getMultiband(getParent1())
    child = TestMultibandUtils.getMultiband(getChild1())
    assert(SoH.getJaccardIndex(parent,child)==1)
    parent = TestMultibandUtils.getMultiband(getParent1())
    child = TestMultibandUtils.getMultiband(getChild6())
    assert(SoH.getJaccardIndex(parent,child)==1/5.0)
  }

  test("Measure structure"){
    val one = SoH.measureStructure(TestMultibandUtils.getMultibandTile1())
    assert(one>0.5)
    val random = SoH.measureStructure(TestMultibandUtils.getMultibandTileRandom())
    assert(random<0.1)

    val parent2 = SoH.measureStructure(TestMultibandUtils.getMultiband(getParent2()))
    assert(parent2<0.3 && parent2>0.2)

  }

  test("Test validation of SoH Multiband"){
    var sohValuesGood = SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getTestClusterParent()),TestMultibandUtils.getMultiband(getTestClusterChild1()))
    println(sohValuesGood)
    var sohValuesBad = SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getTestClusterParent()),TestMultibandUtils.getMultiband(getTestClusterChild2()))
    println(sohValuesBad)
    assert(sohValuesGood._1>sohValuesBad._1 && sohValuesGood._2>sohValuesBad._2)
    var sohValues = SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getSmallParent()),TestMultibandUtils.getMultiband(getSmallChild1()))
    println(sohValues)
    sohValues = SoH.getSoHDowAndUp(getSmallParent(),getSmallChild2())
    println(sohValues)
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild1))==(1,1))
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild2))==(1,0))
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent1()),TestMultibandUtils.getMultiband(getChild3))==(1,1))
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild4))==(1,0))
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild1))==(0,0))
    assert(SoH.getSoHDowAndUp(TestMultibandUtils.getMultiband(getParent2()),TestMultibandUtils.getMultiband(getChild5))==(0.5,1))
  }


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

  def getChild6(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,0,0,0,
      0,2,3,4,5
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
