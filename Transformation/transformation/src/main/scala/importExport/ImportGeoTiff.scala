package importExport

import java.io.File

import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile, TileLayout}
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
  def writeGeoTiff(tile: Tile, file: String, settings: Settings): Unit = {
    SinglebandGeoTiff.apply(tile, new Extent(settings.buttom._1,settings.buttom._2,settings.top._1,settings.top._2), crs).write(file)
  }

  val crs = CRS.fromName("EPSG:3857")

  def geoTiffExists(globalSettings: Settings, extra : String): Boolean = {
    geoTiffExists(getFileName(globalSettings, extra))

  }

  def getFileName(globalSettings: Settings, extra : String): String = {
    ((new PathFormatter).getDirectory(globalSettings, extra) + "aggregation_" + globalSettings.zoomLevel +"w_"+globalSettings.weightRadius+"h_"+globalSettings.hour+".tif")
  }

  def geoTiffExists(file : String): Boolean ={
    new File(file).exists()
  }

  def getGeoTiff(setting : Settings, extra : String): Tile ={
    getGeoTiff(getFileName(setting,extra))
  }

  def getMulitGeoTiff(setting : Settings, extra : String): MultibandTile ={
    getMulitGeoTiff(getFileName(setting,extra))
  }

  def getMulitGeoTiff(file : String): MultibandTile = {
    GeoTiffReader.readMultiband(file)
  }

  def repartitionFiles(file: String, setting: Settings): RDD[(SpatialKey, MultibandTile)] ={
    //val tmp = getMulitGeoTiff(file,setting)
    val sc = SparkContext.getOrCreate(setting.conf)

    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file)

    val layoutScheme = FloatingLayoutScheme(setting.layoutTileSize._1,setting.layoutTileSize._2)
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, crs, layoutScheme)
    val tiled: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, NearestNeighbor)
        .repartition(4)
    tiled
  }

  def readGeoTiff(file : String): Tile = {
    GeoTiffReader.readSingleband(file).tile
  }

  def getGeoTiff(file : String): Tile ={
    getMulitGeoTiff(file).band(0)
  }

  def writeGeoTiff(tile: Tile, settings: Settings, extra : String): Unit = {
    val name = getFileName(settings, extra)
    //println(name)
    writeGeoTiff(tile: Tile,name, settings: Settings)
  }


//  def writeGeoTiff(tile: Tile, para: Settings, file : String): Unit = {
//    writeMultiGeoTiff(MultibandTile(Array(tile)),para,file)
//  }

  def writeMultiGeoTiff(tile: MultibandTile, para: Settings, file : String): Unit = {
    val extent = new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2)
    writeMultiGeoTiff(tile,extent, file)
  }

  def writeMultiGeoTiff(tile: MultibandTile, extent :Extent, file : String): Unit = {
    MultibandGeoTiff.apply(tile, extent,crs).write(file)
  }

  def writeMultiTimeGeoTiffToSingle(tile: MultibandTile, para: Settings, file : String): Unit = {
    for(i <- 0 to tile.bandCount-1){
      SinglebandGeoTiff.apply(tile.band(i), new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2), crs).write(file+"hour_"+i.formatted("%02d")+".tif")
    }
  }

  def writeMultiTimeGeoTiffToSingle(tile: MultibandTile, para: Settings, file : String, origin : Tile): Unit = {
    val layout : TileLayout= new TileLayout(origin.cols,origin.rows,origin.cols,origin.rows)
    for(i <- 0 to tile.bandCount-1){
      val bandTile = tile.band(i) //.split(layout)(0)
      assert(bandTile.dimensions==origin.dimensions)
      SinglebandGeoTiff.apply(bandTile, new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2), crs).write(file+"hour_"+i.formatted("%02d")+".tif")
    }
  }
}
