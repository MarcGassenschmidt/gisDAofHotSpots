package scripts

import geotrellis.raster.{DoubleArrayTile, DoubleRawArrayTile}
import rasterTransformation.NotDataRowTransformation

import scala.io.Source

/**
  * Created by marc on 07.06.17.
  */
class HeatMap {


  def createHeatMap(): Unit ={
    val bufferedSource = Source.fromFile("/home/marc/media/SS_17/output/Heatmap.csv")

    val file = bufferedSource.getLines.drop(1).map(line => {
      val cols = line.split(",").map(_.trim)
      (cols(0).toInt,cols(1).toInt,cols(2).toDouble,cols(3).toDouble)
    })
    val upward = Array.ofDim(10,10)
    val downward = Array.ofDim(10,10)


  }

}
