package importTiff


import geotrellis.raster.{ArrayTile, DoubleRawArrayTile}
import importExport.ImportGeoTiff
import org.scalatest.FunSuite
import parmeters.Settings

/**
  * Created by marc on 05.06.17.
  */
class TestImportGeoTiff extends FunSuite {
  ignore("Test Import/Export"){
    val im = new ImportGeoTiff()

    val par = new Settings()
    var fileName = im.getFileName(par, 0,20, "Test")
    println(fileName)
    im.writeGeoTiff(getTestTile(), par, fileName)
    //fileName = im.getFileName(par,0,20,"T")
    println(fileName)
    val tile = im.getGeoTiff(fileName, par)
    println(tile.asciiDrawDouble())
  }

  def getTestTile(): ArrayTile ={
    val arrayTile = Array[Double](
      0,0,0,1,0,0,0,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      1,1,1,1,1,1,1,
      0,1,1,1,1,1,0,
      0,1,1,1,1,1,0,
      0,0,0,1,0,0,0
    )
    val weightTile = new DoubleRawArrayTile(arrayTile, 7,7)
    weightTile
  }

}
