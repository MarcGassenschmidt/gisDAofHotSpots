package gisOrt

import geotrellis.raster.{IntArrayTile, Tile}
import geotrellis.raster.mapalgebra.focal.{Neighborhood, Square}
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD

/**
  * Created by marc on 27.04.17.
  */
class gisOrt {

  def gStar(layer: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], weight : Array[Int]): Unit ={

    layer.count();
    //val tile = IntArrayTile(layer, 9,4)
    Square(1)

  }

  def xMean(tile : IntArrayTile, focalNeighborhood : Neighborhood): Unit ={
    tile.focalMean(focalNeighborhood)
  }

  def standartDeviation(tile : IntArrayTile, focalNeighborhood : Neighborhood): Unit ={
    tile.standardDeviations()
  }
}
