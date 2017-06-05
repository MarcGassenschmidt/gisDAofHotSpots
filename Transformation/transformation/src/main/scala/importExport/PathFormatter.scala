package importExport

import java.io.File

import org.joda.time.DateTime
import parmeters.Settings

/**
  * Created by marc on 05.06.17.
  */
class PathFormatter {

  def getDirectory(settings : Settings, extra : String): String ={
    var sub = "Time_"+DateTime.now().toString("dd_MM")+"/"
    if(extra.equals("raster")){
      //For alle settings equal
    } else if(settings.focal){
      sub += "focal/"+extra+"/FocalRange_"+settings.focalRange+"/"
    } else {
      sub += "global/"+extra+"/"
    }
    val dir = settings.ouptDirectory+settings.scenario+"/"+sub
    val f = new File(dir)
    f.mkdirs()
    dir
  }

}
