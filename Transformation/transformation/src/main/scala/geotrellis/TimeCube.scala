package geotrellis

import geotrellis.raster.mapalgebra.focal.Neighborhood
import geotrellis.raster.{ArrayMultibandTile, MultibandTile, Tile}

/**
  * Created by marc on 30.06.17.
  */
class TimeCube(_bands: Array[Tile]) extends ArrayMultibandTile(_bands: Array[Tile]){



  def focalCubeSum(neighborhood: TimeNeigborhood): Unit ={
    for(z <- 0 to _bands.length){
      for(x <- 0 to rows-1){
        for(y <- 0 to cols-1){

        }
      }
    }
  }

  def focalCubeStandardDeviation(): Unit ={

  }

  def focalCubeMean(): Unit ={

  }
}

