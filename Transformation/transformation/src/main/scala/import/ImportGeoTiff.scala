package `import`

import java.io.{File, FileOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerDeleter, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.{LayerId, TileLayerMetadata, TileLayerRDD, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods}
import org.joda.time.DateTime
import parmeters.Settings
/**
  * Created by marc on 02.06.17.
  */
class ImportGeoTiff {
  def geoTiffExists(globalSettings: Settings, i: Int, runs: Int, extra : String): Boolean = {
    geoTiffExists(getFileName(globalSettings,i,runs, extra))

  }

  def getFileName(globalSettings: Settings, i: Int, runs: Int, extra : String): String = {
    var sub = "global/"
    if (globalSettings.focal) {
      sub = "focal/" + "FocalRange_" + globalSettings.focalRange + "/"
    }
    val dir = globalSettings.ouptDirectory + globalSettings.scenario + "/" + sub + extra+"/"
    val f = new File(dir)
    f.mkdirs()

    (dir + "_it_" + i + "runs_" + runs + ".tiff")
  }

  def geoTiffExists(file : String): Boolean ={
    new File(file).exists()
  }

  def getGeoTiff(setting : Settings, i : Int, runs : Int, extra : String): Tile ={
    getGeoTiff(getFileName(setting,i,runs,extra),setting)
  }

  def getGeoTiff(file : String, setting : Settings): Tile ={
    if(!geoTiffExists(file)){
      throw new IllegalAccessError("No GeoTiffExist")
    }
    val sc = SparkContext.getOrCreate(setting.conf)
    val inputRdd: RDD[(ProjectedExtent, Tile)] =
      sc.hadoopGeoTiffRDD(file)
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))
    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    tiled.first()._2
  }

  def writeGeoTiff(tile: Tile, settings: Settings, i : Int, runs : Int, extra : String): Unit = {
    writeGeoTiff(tile, settings, getFileName(settings, i, runs, extra))
  }

  def writeGeoTiff(tile: Tile, para: Settings, file : String): Unit = {

    SinglebandGeoTiff.apply(tile, new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2),
      CRS.fromName("EPSG:3857")).write(file)



  }
}
