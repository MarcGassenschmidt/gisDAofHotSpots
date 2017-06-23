package importExport

import sys.process._
import java.net.URL
import java.io.File

import parmeters.Settings
/**
  * Created by marc on 15.05.17.
  */
class DownloadFilesFromWeb {


  def downloadNewYorkTaxiFiles(setting : Settings): Unit ={
    //http://alvinalexander.com/scala/scala-how-to-download-url-contents-to-string-file

    //http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
    //https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv
    //https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-02.csv
    //https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-03.csv
    println("Start Download")
    new URL("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"+setting.csvYear+"-"+setting.csvMonth.formatted("%02d")+".csv") #> new File(setting.inputDirectoryCSV+setting.csvYear+"_"+setting.csvMonth+".csv") !!

    println("End Download")
  }
}