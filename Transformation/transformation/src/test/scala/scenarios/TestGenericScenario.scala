package scenarios

import clustering.ClusterRelations
import geotrellis.raster.{ArrayTile, IntRawArrayTile}
import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by marc on 08.06.17.
  */
class TestGenericScenario extends FunSuite{

  test("test aggregate zoom"){
    val gs =new GenericScenario()
    val tile1 = getTile(8,8)
    val result = gs.aggregateToZoom(tile1,4)
    assert(result.getDouble(0,0)==tile1.toArrayDouble().reduce(_+_))
  }


  test("test aggregate"){
    val gs =new GenericScenario()
    val tile1 = getTile(2,2)
    val result = gs.aggregateTile(tile1)
    assert(result.getDouble(0,0)==tile1.toArrayDouble().reduce(_+_))
  }

  def getTile(cols : Int, rows : Int): ArrayTile ={
    val testTile = Array.fill(rows*cols)(new Random().nextInt(100))
    val rasterTile = new IntRawArrayTile(testTile, cols, rows)
    rasterTile
  }

}
