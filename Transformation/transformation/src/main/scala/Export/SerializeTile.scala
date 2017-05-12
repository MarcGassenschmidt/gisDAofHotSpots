package Export

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import geotrellis.raster.{DoubleArrayTile, Tile}

/**
  * Created by marc on 11.05.17.
  */
class SerializeTile(path : String) {

  def write(tile : Tile): Unit ={
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(tile)
    oos.close
  }

  def read(): Tile = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    val tile = ois.readObject.asInstanceOf[Tile]
    ois.close
    tile
  }
}
