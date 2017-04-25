package rasterTransformation

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import geotrellis.vector._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by marc on 21.04.17.
  */
class Transformation {



  def trasnform(rootPath: Path, config: SparkConf): Unit ={
    val sc = geotrellis.spark.util.SparkUtils.createSparkContext("Test console", config)
    val jobConfig = new JobConf
    val rdd = sc.hadoopRDD(jobConfig,classOf[FileInputFormat[Point,Point]],classOf[Point], classOf[Point])
    

//    /* The `config` argument is optional */
//    val store: AttributeStore = HadoopAttributeStore(rootPath, config)
//
//    val reader = HadoopLayerReader(store)
//    val writer = HadoopLayerWriter(rootPath, store)
  }
}
