package Export

import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, IntRawArrayTile}
import gisOrt.GetisOrd
import org.scalatest.FunSuite

/**
  * Created by marc on 09.05.17.
  */
class TestTileVisualizer extends FunSuite {

  ignore("Test Image Generation"){
    val export = new TileVisualizer()
    export.visualTile(getImageMatrixPositve(), "TestForPositve")
    export.visualTile(getImageMatrixNegativ(), "TestForNegative")
  }

  def getImageMatrixPositve(): ArrayTile = {
    //From R example
    val arrayTile = Array[Double](
      0.1, 0.3, 0.5, 0.3, 0.1,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.5, 1.0, 1.0, 1.0, 0.5,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.1, 0.3, 0.5, 0.3, 0.1)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getImageMatrixNegativ(): ArrayTile = {
    //From R example
    val arrayTile = Array[Double](
      -1, 0.3, 0.5, 0.3, 0.1,
      0.3, 0.8, 1.0, 0.8, 0.3,
      0.5, 1.0, 1.0, 1.0, 0.5,
      0.3, 0.8, 1.0, 0.8, -0.5,
      0.1, 0.3, 0.5, 0.3, -2)
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

}
