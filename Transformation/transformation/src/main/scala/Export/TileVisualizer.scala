package Export

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.FileOutputStream
import javax.imageio.ImageIO

import geotrellis.raster.Tile

/**
  * Created by marc on 08.05.17.
  */
class TileVisualizer {
  def visualTile(tile : Tile, name : String): Unit ={
    val bfI = new BufferedImage(tile.cols, tile.rows, BufferedImage.TYPE_INT_RGB);
    val max = tile.toArrayDouble().max
    val min = tile.toArrayDouble().min
    val rangeFactorRed = 200/(max)
    var rangeFactorBlue = 0.0
    if(min<0){
      rangeFactorBlue = 200/(Math.abs(min))
    }

    val red = new Color(255,0,0)
    val blue = new Color(0,0,255)
    var content : Double = 0.0
    for(i <- 0 to tile.cols-1){
      for(j <- 0 to tile.rows-1){
        content = (tile.getDouble(i,j))
        if(content>0){
          bfI.setRGB(i,j, (new Color((content*rangeFactorRed).ceil.toInt+30,0,0)).getRGB)
        } else {
          bfI.setRGB(i,j, (new Color(0,0,(-1*rangeFactorBlue*content).ceil.toInt+30)).getRGB)
        }

      }
    }
    val fos = new FileOutputStream("/home/marc/Masterarbeit/outPut/"+name+".png");
    ImageIO.write(bfI, "PNG", fos);
    fos.close();


  }
}
