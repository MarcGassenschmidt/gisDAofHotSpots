package clustering

import com.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap


/**
  * Created by marc on 19.04.17.
  */
class Cluster {
  def test(sparkContext : SparkContext, path : String): Unit = {
    //TODO possible to set the partitions
    val data = sparkContext.wholeTextFiles(path)
    val files = data.map { case (filename, content) => filename}
    val resultMap = new TreeMap[Int, RDD[Row]]()
    val filteredData = files.map(file => getOneFile(sparkContext, file))
    //mapUnion(resultMap, filteredData)
    //TODO other Context

    println("End TODO saving")






  }

  def mapUnion(resultMap: TreeMap[Int, RDD[Row]], filteredData: RDD[TreeMap[Int, RDD[Row]]]): TreeMap[Int, RDD[Row]] = {
    for (oneData <- filteredData) {
      for (oneSlice <- oneData) {
        if (resultMap.contains(oneSlice._1)) {
          resultMap.insert(oneSlice._1, oneSlice._2.union(resultMap(oneSlice._1)))
        } else {
          resultMap.insert(oneSlice._1, oneSlice._2)
        }
      }
    }
    resultMap
  }

  def getOneFile(sparkContext: SparkContext, fileName : String): Unit = {//TreeMap[Int, RDD[Row]] = {
      val monthFile = sparkContext.textFile(fileName);
      val withoutHeader: RDD[String] = dropHeader(monthFile)
      var counter = 0;
      val usedValues = monthFile.mapPartitions(lines => {
        val parser = new CSVParser(',')
        val map = lines.map(line => {
          val cols = line.split(",").map(_.trim)
          val result = new Row(counter, cols(5).toFloat, cols(6).toFloat)
          counter += 1
          result
        })

        map
      })

//      val numberNodse = 4
//      val sqrtNodes = Math.sqrt(numberNodse).toInt
//      val latMin = usedValues.map(row => row.lat).min
//      val lonMin = usedValues.map(row => row.lon).min
//      val latMax = usedValues.map(row => row.lat).max
//      val lonMax = usedValues.map(row => row.lon).max
//      val partionSizeLat = (latMin + latMax) / sqrtNodes
//      val partionSizeLon = (lonMin + lonMax) / sqrtNodes
//      //TODO Hive query with Cluster by
//      val resultMap = new TreeMap[Int, RDD[Row]]()
//      for (i <- 1 to sqrtNodes) {
//        for (j <- 1 to sqrtNodes) {
//          resultMap.insert((i + i * j), usedValues.filter(row => row.lat < partionSizeLat * j && row.lon < partionSizeLon * i))
//        }
//
//      }
//      resultMap

  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(2)
      }
      lines
    })
  }
}
