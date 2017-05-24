package getisOrd

import org.scalatest.FunSuite

/**
  * Created by marc on 24.05.17.
  */
class TestGenericGetisOrd extends FunSuite {

  test("Test Sigmoid Square"){
    val g = new GenericGetisOrd()
    println(g.getWeightMatrixSquareSigmoid(3,1).asciiDrawDouble())
  }
}
