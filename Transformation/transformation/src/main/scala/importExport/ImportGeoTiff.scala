package importExport

import java.io.File

import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parmeters.Settings
import geotrellis.spark.io.hadoop.{HadoopAttributeStore, HadoopLayerReader, HadoopLayerWriter, HadoopSparkContextMethodsWrapper}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, withProjectedExtentTilerKeyMethods, withTileRDDReprojectMethods, withTilerMethods, _}
/**
  * Created by marc on 02.06.17.
  */
class ImportGeoTiff {
  val crs = CRS.fromName("EPSG:3857")
  val layoutScheme = FloatingLayoutScheme(50)
  def geoTiffExists(globalSettings: Settings, i: Int, runs: Int, extra : String): Boolean = {
    geoTiffExists(getFileName(globalSettings,i,runs, extra))

  }

  def getFileName(globalSettings: Settings, i: Int, runs: Int, extra : String): String = {
    ((new PathFormatter).getDirectory(globalSettings, extra) + "it_" + i + "runs_" + runs +"w_"+globalSettings.weightRadius+"h_"+globalSettings.hour+".tif")
  }

  def geoTiffExists(file : String): Boolean ={
    new File(file).exists()
  }

  def getGeoTiff(setting : Settings, i : Int, runs : Int, extra : String): Tile ={
    getGeoTiff(getFileName(setting,i,runs,extra),setting)
  }

  def getMulitGeoTiff(setting : Settings, i : Int, runs : Int, extra : String): MultibandTile ={
    getMulitGeoTiff(getFileName(setting,i,runs,extra),setting)
  }

  def getMulitGeoTiff(file : String, setting : Settings): MultibandTile = {
    GeoTiffReader.readMultiband(file)
  }

  def repartitionFiles(file: String, setting: Settings): RDD[(SpatialKey, MultibandTile)] ={
    val sc = SparkContext.getOrCreate(setting.conf)
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file)
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, crs, layoutScheme)
    val tiled: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, NearestNeighbor)
        .repartition(16)
    tiled
  }

  def getGeoTiff(file : String, setting : Settings): Tile ={
    getMulitGeoTiff(file,setting).band(0)
  }

  def writeGeoTiff(tile: Tile, settings: Settings, i : Int, runs : Int, extra : String): Unit = {
    val name = getFileName(settings, i, runs, extra)
    //println(name)
    writeGeoTiff(tile, settings, name)
  }

  def writeMulitGeoTiff(tile: MultibandTile, settings: Settings, i : Int, runs : Int, extra : String): Unit = {
    val name = getFileName(settings, i, runs, extra)
    //println(name)
    writeMulitGeoTiff(tile, settings, name)
  }

  def writeGeoTiff(tile: Tile, para: Settings, file : String): Unit = {
    writeMulitGeoTiff(MultibandTile(Array(tile)),para,file)
  }

  def writeMulitGeoTiff(tile: MultibandTile, para: Settings, file : String): Unit = {
    MultibandGeoTiff.apply(tile, new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2),
      crs).write(file)
  }
}
