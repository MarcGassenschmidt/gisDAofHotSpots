package demo

import java.sql.Connection
import java.util

import export.{SerializeTile, SoHResult, SoHResultTabell, TileVisualizer}
import clustering.{Cluster, ClusterHotSpots}
import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.{SparkConf, SparkContext}
import db.{ImportToDB, QueryDb}
import getisOrd.Weight._
import getisOrd.{GetisOrd, GetisOrdFocal, SoH, Weight}
import rasterTransformation.Transformation
import scenarios.DifferentRasterSizes

import scala.collection.mutable.ListBuffer
import scala.slick.driver.PostgresDriver.simple._

object Main {
  def helloSentence = "Start"

  def main(args: Array[String]): Unit = {
    val scenario = new DifferentRasterSizes()
    scenario.runScenario()
  }

}
