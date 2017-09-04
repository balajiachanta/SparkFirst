package com.bala.spark

import org.apache.log4j._
import org.apache.spark.rdd.RDD


case class LogSchema(address: String,
		delim:String,
		datetime: String,
		action: String,
		delim2: String,
		status:String
		)

class TransformMapper{
	def transform(events: RDD[LogSchema]) = {

			//val e = events.map(x => x.action.filter("/server-status"))

					// val e = events.map(x => (if(x.action.indexOf("?") > 0) x.action.split("\\?")(0) else x.action, 1)).reduceByKey (_ + _)

			    val e = events.map(x => if(x.action.indexOf("?") > 0) x.action.split("\\?")(0) else x.action)
					val x = e.filter(f => !f.equals("/keepalive.html"))
					val se=  x.filter(f => !f.equals("/server-status"))
					val hy=  x.filter(se => !se.equals("-"))
      	//  se.foreach(x => println(x))
					val results = hy.countByValue();
          val sortedResults = results.toSeq.sortBy(_._2)
          
           println("\nendpoint stats\n")
      	sortedResults.foreach(println)
      	
      	val status = events.map(x => (x.status,1)).reduceByKey (_ + _)

					 println("\n\n\nserver response stats\n")
      	status.foreach(println)

	}
	
		def transformStatus(events: RDD[LogSchema]) = {

			
			    val e = events.map(x => (x.status,1)).reduceByKey (_ + _)

					
      	e.foreach(println)

	}

	def findEx(li :String) : String = { if(li.indexOf("?") > 1) li.split("?")(0) else li

	}
}

object MapRawData extends Serializable{
	def mapRawLine(line: String): Option[LogSchema] = {
			try {
				val fields = line.split("\\|", -1).map(_.trim)
						Some(
								LogSchema(
										address = fields(0),
										delim=fields(1),
										datetime = fields(2),
										action = fields(3).toString(),
                    delim2=fields(4),
                    status=fields(5)

										)
								)
			}
			catch {
			case e: Exception =>

			None
			}
	}
}