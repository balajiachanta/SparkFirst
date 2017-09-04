package com.bala.spark

import org.apache.spark._

import org.joda.time.DateTime
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.log4j._

object SparkJDBC {
	def main(args: Array[String]): Unit = {
			val log = LogManager.getRootLogger

					Logger.getLogger("org").setLevel(Level.ERROR)
					// val conf = new SparkConf().setAppName("spark-loganalyzer").setMaster("local[2]").set("spark.executor.memory","1g")
					// val sc = new SparkContext(conf)
					val sc = new SparkContext("local[*]", "SparkJDBC")
					val startTimeJob = new DateTime(sc.startTime)
					val applicationId = sc.applicationId
					// log.info("Application launched with id : " + applicationId)

					val rawData = sc.textFile("./logfile.txt")
					log.info(rawData)
					//val numberOfRawLines = rawData.count()
				//	log.info("Number of lines to parse : " + numberOfRawLines)
	//rawData.foreach { x => println(x.toString()) }

					val names= rawData.map  { x => x.toString().split("]|")(1)}

		

			// Print each result on its own line.
			// results.foreach()

	}
}

