package dataStrukture

import geotrellis.raster.{DoubleArrayTile, DoubleCells, DoubleRawArrayTile, NoDataHandling}

/**
  * Created by marc on 09.05.17.
  */
class RootTile(arr: Array[Double], cols: Int, rows: Int) extends DoubleArrayTile(arr, cols, rows) {
  override val cellType: DoubleCells with NoDataHandling = _

  override def update(i: Int, z: Int): Unit = ???

  override def updateDouble(i: Int, z: Double): Unit = ???

  override def apply(i: Int): Int = ???

  override def applyDouble(i: Int): Double = ???

  override def cols: Int = this.cols

  override def rows: Int = this.rows
}
