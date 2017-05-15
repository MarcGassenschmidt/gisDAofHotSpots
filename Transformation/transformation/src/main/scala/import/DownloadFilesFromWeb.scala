package `import`

import sys.process._
import java.net.URL
import java.io.File
/**
  * Created by marc on 15.05.17.
  */
class DownloadFilesFromWeb {


  def downloadNewYorkTaxiFiles(): Unit ={
    //http://alvinalexander.com/scala/scala-how-to-download-url-contents-to-string-file
    //http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
    new URL("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv") #> new File("in.csv") !!
  }
}
