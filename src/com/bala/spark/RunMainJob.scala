package com.bala.spark

import org.apache.spark._
import org.joda.time.DateTime
import org.apache.log4j._


object RunMainJob extends TransformMapper {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RunMainJob")
    val startTimeJob = new DateTime(sc.startTime)
    val applicationId = sc.applicationId
   

    val rawData = sc.textFile("./logfile.txt")
    val numberOfRawLines = rawData.count()
    

    val mapRawData = MapRawData
    val parseData = rawData.flatMap(x => mapRawData.mapRawLine(x))
  // parseData.foreach { x => println(x) }

   
    transform(parseData)
    
    
   // transformStatus(parseData)
  }
}