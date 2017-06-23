package importExport

import java.io.File

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.Extent
import parmeters.Settings
/**
  * Created by marc on 02.06.17.
  */
class ImportGeoTiff {
  def geoTiffExists(globalSettings: Settings, i: Int, runs: Int, extra : String): Boolean = {
    geoTiffExists(getFileName(globalSettings,i,runs, extra))

  }

  def getFileName(globalSettings: Settings, i: Int, runs: Int, extra : String): String = {


    ((new PathFormatter).getDirectory(globalSettings, extra) + "it_" + i + "runs_" + runs + ".tiff")
  }

  def geoTiffExists(file : String): Boolean ={
    new File(file).exists()
  }

  def getGeoTiff(setting : Settings, i : Int, runs : Int, extra : String): Tile ={
    getGeoTiff(getFileName(setting,i,runs,extra),setting)
  }

  def getGeoTiff(file : String, setting : Settings): Tile ={
    GeoTiffReader.readSingleband(file)
//    if(!geoTiffExists(file)){
//      throw new IllegalAccessError("No GeoTiffExist:"+file)
//    }
//    val sc = SparkContext.getOrCreate(setting.conf)
//    val inputRdd: RDD[(ProjectedExtent, Tile)] =
//      sc.hadoopGeoTiffRDD(file)
//    val (_, rasterMetaData) =
//      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))
//    val tiled: RDD[(SpatialKey, Tile)] =
//      inputRdd
//        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
//        .repartition(1)
//    tiled.first()._2
  }

  def writeGeoTiff(tile: Tile, settings: Settings, i : Int, runs : Int, extra : String): Unit = {
    val name = getFileName(settings, i, runs, extra)
    //println(name)
    writeGeoTiff(tile, settings, name)
  }

  def writeGeoTiff(tile: Tile, para: Settings, file : String): Unit = {
    SinglebandGeoTiff.apply(tile, new Extent(para.buttom._1,para.buttom._2,para.top._1,para.top._2),
      CRS.fromName("EPSG:3857")).write(file)
  }
}
