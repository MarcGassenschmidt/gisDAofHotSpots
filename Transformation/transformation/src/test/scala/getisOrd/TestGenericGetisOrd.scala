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

  test("G* full case"){
    val RoW = getTestMatrix().focalSum(new Circle(1)) //Todo if different Weight
    println(RoW.asciiDrawDouble())

    val M = getTestMatrix().focalMean(new Circle(2))
    assert(M.getDouble(0,0)==4.0/6.0)
    println(M.asciiDrawDouble())
    val MW = M*getParentWeigth().toArrayDouble().reduce(_+_)
    assert((MW-(M*5)).toArrayDouble().reduce(_+_)==0)
    println(MW.asciiDrawDouble())
    val S = getSD()
    println(S.asciiDrawDouble())
    val N = getTestMatrix().focalSum(new Circle(2))
    println(N.asciiDrawDouble())

    val W = getParentWeigth().toArrayDouble().foldLeft(0.0){(x,y)=>x+y*y}
    val mW = Math.pow(getParentWeigth().toArrayDouble().reduce(_+_),2)
    println(W)
    println(mW)
    val WmW =N*W-mW
    println(WmW.asciiDrawDouble())
    val numerator = (RoW-MW)
    val denumerator = (S*(WmW/(N-1)).mapDouble(x => Math.sqrt(Math.max(0,x))))
    println(numerator.asciiDrawDouble())
    println(denumerator.asciiDrawDouble())
    val gStar = numerator/denumerator
    println(gStar.asciiDrawDouble())
    val setting = new Settings
    setting.focalRange=2
    setting.weightRadius = 1
    val getisOrdFocal = new GetisOrdFocal(getTestMatrix(),setting)
    val r = getisOrdFocal.debugFocalgStar()
    assert(getisOrdFocal.sumOfWeight==5)

    testSD(S, r)
    assert((r._6-RoW).toArrayDouble().reduce(_+_)==0.0)

    println((r._7-MW).toArrayDouble().reduce(_+_)==0)

    testForNumerator(numerator, r)
    testDenumerator(denumerator, r)
    assert((r._5-N).toArrayDouble().reduce(_+_)==0.0)


    println((r._1-gStar).asciiDrawDouble())
  }


  def testDenumerator(denumerator: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {


    //println((r._4 - denumerator).asciiDrawDouble())
    //Because Test does not handel ND values
    assert((r._4 - denumerator).toArrayDouble().reduce(_ + _) < 1.11803398+0.01 && (r._4 - denumerator).toArrayDouble().reduce(_ + _) > 1.11803398-0.01)
  }

  def testSD(S: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {
    println(r._3.asciiDraw())
    println(S.asciiDraw())
    assert((r._3 - S).toArrayDouble().reduce(_ + _) ==0.0)
  }

  def testForNumerator(numerator: Tile, r: (Tile, Tile, Tile, Tile, Tile,Tile,Tile)): Unit = {
//    println(r._2.asciiDrawDouble())
//    println(numerator.asciiDrawDouble())
    println((r._2-numerator).asciiDrawDouble())
    assert((r._2-numerator).toArrayDouble().reduce(_+_)==0.0)
  }

  def getParentWeigth(): ArrayTile ={
    val arrayTile = Array[Double](
      0,1,0,
      1,1,1,
      0,1,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 3,3)
    weightTile
  }

  def getChildWeigth(): ArrayTile ={
    val arrayTile = Array[Double](
      1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 1,1)
    weightTile
  }

  def getFocalRadius(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,0,0,
      0,1,1,1,0,
      1,1,1,1,1,
      0,1,1,1,0,
      0,0,1,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 5,5)
    weightTile
  }

  def getXMean(): ArrayTile ={
    val arrayTile = Array[Double](
      1/3,3/4,1002/4,1,
      3/4,1003/5,1007/5,1003/4,
      2/4,1003/5,1006/5,1003/4,
      1,1,1003/4,1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 4,5)
    weightTile
  }

  def getTestMatrix(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,1,1,
      1,2,1000,1,
      0,0,3,1,
      1,1,1000,1,
      1,1,1,1
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 4,5)
    weightTile
  }

  def getSD(): Tile ={
    val sd = getTestMatrix().focalStandardDeviation(new Circle(2))
    println(sd.asciiDrawDouble())
    //assert(sd.getDouble(0,0)==143) //TODO why?
    sd
  }

}
