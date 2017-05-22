package getisOrd

import java.util.Random

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.mapalgebra.focal.Circle
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, IntRawArrayTile, Tile}
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
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.index.ZCurveKeyIndexMethod.spatialKeyIndexMethod
import geotrellis.spark.io.{SpatialKeyFormat, spatialKeyAvroFormat, tileLayerMetadataFormat, tileUnionCodec}
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.SparkConf
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
    assert(rasterTile.focalMean(Circle(setting.focalRange)).getDouble(0,0)==sum/6)
  }

  test("Test focal Mean row,cols"){
    getis.setFocalRadius(1)
    val sum = (rasterTile.get(9,9)+rasterTile.get(8,9)+rasterTile.get(9,8))
    assert(rasterTile.focalMean(Circle(setting.focalRange)).getDouble(9,9)==sum/3)
  }

  test("Test focal Mean"){
    getis.setFocalRadius(1)
    val sum = rasterTile.get(5,5)+rasterTile.get(5,6)+rasterTile.get(6,5)+rasterTile.get(4,5)+rasterTile.get(5,4)
    assert(rasterTile.focalMean(Circle(setting.focalRange)).getDouble(5,5)==sum/5.0)
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



  test("Test Focal G*"){
//    setting.focalRange = 5
//    val rnd = new Random(1)
//    val testTile = Array.fill(1000000)(rnd.nextInt(100))
//    rasterTile = new IntRawArrayTile(testTile, 1000, 1000)
//    SinglebandGeoTiff.apply(rasterTile, new Extent(0, 0, rasterTile.cols, rasterTile.rows),
//      CRS.fromName("EPSG:3005")).write(setting.ouptDirectory+"geoTiff.tiff")
//
//    implicit val sc : SparkContext = SparkContext.getOrCreate(setting.conf)
//    val config : SparkConf = setting.conf
//    val inputRdd = sc.hadoopGeoTiffRDD(setting.ouptDirectory+"geoTiff.tiff")
//    val (_, myRasterMetaData) = TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme())
//    val tiled = inputRdd.tileToLayout(myRasterMetaData.cellType, myRasterMetaData.layout)
//
//    val catalogPathHdfs = new Path(setting.ouptDirectory+"spark/")
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
//    getis = new GetisOrdFocal(test, setting)
//    println(getis.gStarComplete().getDouble(0,0))

  }

}
