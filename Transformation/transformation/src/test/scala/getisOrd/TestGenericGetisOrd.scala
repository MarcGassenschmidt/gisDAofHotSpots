package getisOrd

import geotrellis.raster.mapalgebra.focal.Circle
import geotrellis.raster.{ArrayTile, DoubleRawArrayTile, Tile}
import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 24.05.17.
  */
class TestGenericGetisOrd extends FunSuite {

  test("Test Sigmoid Square"){
    val g = new GenericGetisOrd()
    println(g.getWeightMatrixSquareSigmoid(3,1).asciiDrawDouble())
  }



}
