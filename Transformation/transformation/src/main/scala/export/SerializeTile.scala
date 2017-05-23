package export

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, MultibandTile, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parmeters.Settings
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.withTilerMethods
/**
  * Created by marc on 11.05.17.
  */
class SerializeTile(path : String) {

  def writeGeoTiff(tile : Tile, setting : Settings) : Unit = {
    SinglebandGeoTiff.apply(tile, new Extent(0, 0, tile.cols, tile.rows),
      CRS.fromName("EPSG:3005")).write(setting.ouptDirectory+"geoTiff.tiff")
  }

  def readGeoTiff( setting : Settings): Tile ={
    val sc = SparkContext.getOrCreate(setting.conf)
    val inputRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(setting.ouptDirectory+"geoTiff.tiff")
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))
    val tiled: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    tiled.count()
    return inputRdd.take(1)(0)._2
  }

  def write(tile : Tile): Unit ={
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(tile)
    oos.close
  }

  def read(): Tile = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    val tile = ois.readObject.asInstanceOf[Tile]
    ois.close
    tile
  }
}
