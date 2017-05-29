package getisOrd

import java.util.Random

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.focal.Circle
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, DoubleRawArrayTile,IntArrayTile, IntRawArrayTile, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.tiling.{FloatingLayoutScheme, Tiler}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import parmeters.Settings
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.WebMercator
import geotrellis.raster.withTileMethods
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.SparkConf
import geotrellis.spark.withTilerMethods
import geotrellis.spark.tiling.FloatingLayoutScheme
/**
  * Created by marc on 10.05.17.
  */
class TestGetisOrdFocal extends FunSuite with BeforeAndAfter {

  var getis : GetisOrdFocal = _
  var rasterTile : DoubleRawArrayTile = _
  var setting = new Settings()
  before {
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(100)(rnd.nextInt(100))
    rasterTile = new DoubleRawArrayTile(testTile, 10, 10)
    setting.focalRange = 2
    getis = new GetisOrdFocal(rasterTile, setting)

    setting.weightMatrix = Weight.One
    getis.createNewWeight(setting)
  }

  test("Test focal Mean 0,0"){
    val sum = (rasterTile.get(0,0)+rasterTile.get(0,1)+rasterTile.get(1,0)+rasterTile.get(1,1)+rasterTile.get(2,0)+rasterTile.get(0,2))
    assert(rasterTile.focalMean(Circle(setting.focalRange)).getDouble(0,0)==sum/6)
  }

  test("Test focal Mean row,cols"){
    val sum = (rasterTile.get(9,9)+rasterTile.get(8,9)+rasterTile.get(9,8))
    assert(rasterTile.focalMean(Circle(1)).getDouble(9,9)==sum/3)
  }

  test("Test focal Mean"){
    val sum = rasterTile.get(5,5)+rasterTile.get(5,6)+rasterTile.get(6,5)+rasterTile.get(4,5)+rasterTile.get(5,4)
    assert(rasterTile.focalMean(Circle(1)).getDouble(5,5)==sum/5.0)
  }



  test("Test other focal SD"){
    setting.focalRange = 1
    getis = new GetisOrdFocal(rasterTile, setting)
    val index = (5,5)
    val xMean =rasterTile.focalMean(Circle(setting.focalRange)).getDouble(index._1, index._2)
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

    assert(rasterTile.focalStandardDeviation(Circle(setting.focalRange)).getDouble(5,5)==sd)
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


  ignore("SparkReadingWriting"){
    setting.focalRange = 5
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(1000000)(rnd.nextInt(100))
    rasterTile = new DoubleRawArrayTile(testTile, 1000, 1000)
    SinglebandGeoTiff.apply(rasterTile, new Extent(0, 0, rasterTile.cols, rasterTile.rows),
      CRS.fromName("EPSG:3005")).write(setting.ouptDirectory+"geoTiff.tiff")
    val sc = SparkContext.getOrCreate(setting.conf)
    val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(setting.ouptDirectory+"geoTiff.tiff")
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))
    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    println(tiled.count())
    println(tiled.first()._2.cols)
    println(tiled.first()._1.toString)
    for(t <- tiled){
      println(t._2.cols)
      println(t._1.toString)
    }

    //println(inputRdd.take(1)(0)._2.asciiDraw())
  }

  test("Test Focal G*"){
    setting.focalRange = 3
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(900)(rnd.nextDouble()*10)
    val testTile2: Array[Double] = Array.fill(9)(3.0)
    val rasterTile1 = new DoubleRawArrayTile(testTile, 30, 30)
    val rasterTile2 = new DoubleRawArrayTile(testTile2, 3, 3)
   // println((rasterTile1/rasterTile2).mapDouble(x => Math.sqrt(x)).asciiDrawDouble())
   // println(rasterTile.focalStandardDeviation(new Circle(1)).asciiDrawDouble())

    setting.weightRadius = 2
    setting.weightMatrix = Weight.Square
    var getis = new GetisOrdFocal(rasterTile1, setting)

    println(getis.gStarComplete().asciiDrawDouble())

  }



  test("Test negative Focal G*"){
    setting.focalRange = 3
    val rnd = new Random(1)
    val testTile : Array[Double]= Array.fill(900)(-1*rnd.nextDouble()*10)
    val testTile2: Array[Double] = Array.fill(9)(3.0)
    val rasterTile1 = new DoubleRawArrayTile(testTile, 30, 30)
    val rasterTile2 = new DoubleRawArrayTile(testTile2, 3, 3)
    // println((rasterTile1/rasterTile2).mapDouble(x => Math.sqrt(x)).asciiDrawDouble())
    // println(rasterTile.focalStandardDeviation(new Circle(1)).asciiDrawDouble())

    setting.weightRadius = 2
    setting.weightMatrix = Weight.Square
    var getis = new GetisOrdFocal(rasterTile1, setting)

    println(getis.gStarComplete().asciiDrawDouble())

  }

  def notWorking(sc: SparkContext): Unit = {
//    val config: SparkConf = setting.conf
//    val inputRdd = sc.hadoopGeoTiffRDD(setting.ouptDirectory + "geoTiff.tiff")
//    val (_, myRasterMetaData) = TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme())
//    val tiled = inputRdd.tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout)
//
//    val catalogPathHdfs = new Path(setting.ouptDirectory + "spark/")
//    val attributeStore = HadoopAttributeStore(catalogPathHdfs)
//
//    // Create the writer that we will use to store the tiles in the local catalog.
//    val writer = HadoopLayerWriter(catalogPathHdfs, attributeStore)
//    val layerId = new LayerId("Test", 1)
//
//    val reader = HadoopLayerReader(attributeStore)
//
//    val queryResult: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = layerReader
//      .read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](attributeStore.layerIds(1))
//    val test = queryResult.take(1)(1)._2
//    println(test.asciiDraw())
//    test
  }

  def run(implicit sc: SparkContext, inputPath : String, outputPath : String) = {
    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
    sc.hadoopMultibandGeoTiffRDD(inputPath)

    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    val (_, rasterMetaData) =
    TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiled: RDD[(SpatialKey, MultibandTile)] =
    inputRdd
      .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
      .repartition(100)

    // We'll be tiling the images using a zoomed layout scheme
    // in the web mercator format (which fits the slippy map tile specification).
    // We'll be creating 256 x 256 tiles.
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // We need to reproject the tiles to WebMercator
    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
    MultibandTileLayerRDD(tiled, rasterMetaData)
      .reproject(WebMercator, layoutScheme, Bilinear)

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId("landsat", z)
      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }

  test("G* full case"){
    val RoW = getTestMatrix().focalSum(new Circle(1)) //Todo if different Weight
    println(RoW.asciiDrawDouble())

    val M = getTestMatrix().focalMean(new Circle(2))
    assert(M.getDouble(0,0)==4.0/6.0)
    println(M.asciiDrawDouble())
    val MW = M*getParentWeigth().toArrayDouble().reduce(_+_)
    println(MW.asciiDrawDouble())
    println((M*5.4).asciiDrawDouble())
    assert((MW-(M*5.4)).toArrayDouble().reduce(_+_)<0.001 && (MW-(M*5.4)).toArrayDouble().reduce(_+_)> -0.001)
    println(MW.asciiDrawDouble())
    val S = getSD()
    println(S.asciiDrawDouble())
    val N = getTestMatrix().focalSum(new Circle(2))
    println(N.asciiDrawDouble())

    val W = getParentWeigth().toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
    val mW = Math.pow(getParentWeigth().toArrayDouble().reduce(_+_),2)
    println(W)
    println(mW)
    val WmW =N*W-mW
    println(WmW.asciiDrawDouble())
    val numerator = (RoW-MW)
    val denumerator = (S*(WmW/(N-1)).mapDouble(x => Math.sqrt(Math.max(0,x))))
    println(numerator.asciiDrawDouble())
    println(denumerator.asciiDrawDouble())
    val gStar = numerator/denumerator

    println(gStar.asciiDrawDouble())
    val setting = new Settings
    setting.focalRange=2
    setting.weightRadius = 1
    setting.weightMatrix= Weight.Defined
    val getisOrdFocal = new GetisOrdFocal(getTestMatrix(),setting)
    val r = getisOrdFocal.debugFocalgStar()
    assert(getisOrdFocal.sumOfWeight>5.3 &&getisOrdFocal.sumOfWeight<5.4)

    testSD(S, r)
    assert((r._6-RoW).toArrayDouble().reduce(_+_)==0.0)

    println((r._7-MW).toArrayDouble().reduce(_+_)==0)

    testForNumerator(numerator, r)
    testDenumerator(denumerator, r)
    assert((r._5-N).toArrayDouble().reduce(_+_)==0.0)

    println((r._1-gStar).asciiDrawDouble())
    println(r._1.asciiDrawDouble())
    val tileG = DoubleArrayTile.ofDim(numerator.cols, numerator.rows)
    for(i <- 0 to gStar.cols-1){
      for(j <- 0 to gStar.rows-1){
        val qt : Double = gStar.getDouble(i,j)
        if(qt.equals(Double.NaN)){
          tileG.setDouble(i,j,0)
        } else {
          tileG.setDouble(i,j,gStar.getDouble(i,j))
        }
      }
    }
    println(tileG.asciiDrawDouble())
    assert((r._1-tileG).toArrayDouble().reduce(_+_) ==0.0)
    assert((getisOrdFocal.gStarComplete()-tileG).toArrayDouble().reduce(_+_) ==0.0)
  }


  def testDenumerator(denumerator: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {

    println((denumerator).asciiDrawDouble())
    println((r._4 - denumerator).asciiDrawDouble())

    //Because Test does not handel ND values
    assert((r._4 - denumerator).toArrayDouble().reduce(_ + _)==0)
  }

  def testSD(S: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {
    println(r._3.asciiDraw())
    println(S.asciiDraw())
    assert((r._3 - S).toArrayDouble().reduce(_ + _) ==0.0)
  }

  def testForNumerator(numerator: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {
    //    println(r._2.asciiDrawDouble())
    //    println(numerator.asciiDrawDouble())
    println((r._2-numerator).asciiDrawDouble())
    assert((r._2-numerator).toArrayDouble().reduce(_+_)==0.0)
  }

  def getParentWeigth(): ArrayTile ={
    val arrayTile = Array[Double](
      0.1,1,0.1,
      1,1,1,
      0.1,1,0.1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 3,3)
    weightTile
  }

  def getChildWeigth(): ArrayTile ={
    val arrayTile = Array[Double](
      1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 1,1)
    weightTile
  }

  def getFocalRadius(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,0,0,
      0,1,1,1,0,
      1,1,1,1,1,
      0,1,1,1,0,
      0,0,1,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getXMean(): ArrayTile ={
    val arrayTile = Array[Double](
      1/3,3/4,1002/4,1,
      3/4,1003/5,1007/5,1003/4,
      2/4,1003/5,1006/5,1003/4,
      1,1,1003/4,1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 4,5)
    weightTile
  }

  def getTestMatrix(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,1,
      1,2,1000,1,
      0,0,3,1,
      1,1,1000,1,
      1,1,1,1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 4,5)
    weightTile
  }

  def getSD(): Tile ={
    val sd = getTestMatrix().focalStandardDeviation(new Circle(2))
    println(sd.asciiDrawDouble())
    //assert(sd.getDouble(0,0)==143) //TODO why?
    sd
  }

}
