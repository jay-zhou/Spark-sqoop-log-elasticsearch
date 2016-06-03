package com.edl.bto.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql

import java.io.File
import org.elasticsearch.spark._ 
import org.elasticsearch.spark.sql._


/*
 * Author: Jay Zhou
 * Date: May 2016
 * 
 */


object SqoopLog {
  
  
  case class SqoopLogFields(mapTime: Double,
                            gcTime: Double,
                            cpuTime: Double,
                            totMemory: Long,
                            fileSize: Long,
                            transferTime: Double )
                            
  case class Metrics_Sqoop(moduleCode: String,
                           bDate: String,
                           tblName: String,
                           mapTime: Double,
                           gcTime: Double,
                           cpuTime: Double,
                           totMemory: Long,
                           fileSize: Long,
                           transferTime: Double )
                            
                           
  // Get files under the directory                          
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
  }
  
  
  // Parse the sqoop log file to extract some info
  def parseLogs(line: String): SqoopLogFields = {
    
    var mapOccupTime = 0.0
    var mapTasksTime = 0.0
    var numRecords = 0
    var gcTime = 0.0
    var cpuTime = 0.0
    var phyMemory = 0L
    var virMemory = 0L
    var fileSize = 0L
    var transferTime = 0.0
    
    if (line.contains("Total time spent by all maps in occupied slots")) {
      mapOccupTime = line.split("=")(1).toDouble
    }
    else if (line.contains("Total time spent by all map tasks")) {
      mapTasksTime = line.split("=")(1).toDouble
    }
    else if (line.contains("Map input records=")) {
      numRecords = line.split("=")(1).toInt
    }
    else if (line.contains("GC time elapsed (ms)=")) {
      gcTime = line.split("=")(1).toDouble
    }
    else if (line.contains("CPU time spent (ms)=")) {
      cpuTime = line.split("=")(1).toDouble
    }
    else if (line.contains("Physical memory (bytes) snapshot=")) {
      phyMemory = line.split("=")(1).toLong
    }
    else if (line.contains("Virtual memory (bytes) snapshot=")) {
      virMemory = line.split("=")(1).toLong
    }
    else if (line.contains("Bytes Written=")) {
      fileSize = line.split("=")(1).toLong
    }
    else if (line.contains("mapreduce.ImportJobBase: Transferred")) {
      transferTime = line.split(" ")(8).toDouble
    }
    
    SqoopLogFields((mapOccupTime+mapTasksTime)/1000.0, gcTime/1000.0, cpuTime/1000.0, phyMemory+virMemory, fileSize, transferTime)
       
  }

  
  
  def getTable(file: String): String = {
    
    // File name is like this format: ppls_tkq_authorization_log_txn_2016-04-29.log
    val fileNameTemp = file.split("\\.")(0).split("-")(0)
    val index = fileNameTemp.lastIndexOf("_")
    val fileName = fileNameTemp.substring(0, index)
    fileName
  }
  
  
  
  def getRunningDate(file: String): String = {
    
    // File name is like this format: ppls_tkq_authorization_log_txn_2016-04-29.log
    val runningDate = file.split("\\.")(0).split("_").last
    runningDate    
  }
  
    
  
  def main(args: Array[String]) = {
    
        
    if (args.length < 1) {
      System.err.println("Usage: SqoopLog ingest_dir")
      System.exit(1)
    }
     
     
    // Test to set the default directory
    val baseDir = "C:/appdata/sqoop_log"
    
    
    val moduleList = List("kq1", "kq2", "jg1", "jg2", "jg3", "jg4" )
    
    
    val conf = new SparkConf().setAppName("Sqoop Log Analysis Application").setMaster("local")
                              .set("es.nodes", "localhost")
                              .set("es.port","9201")
                              .set("es.http.timeout","5m")
                              .set("es.scroll.size","50")
                              .set("es.index.auto.create", "true")
                           //   .set("spark.es.resource","index/spark")
                             
                              
                              
    
    val sc = new SparkContext(conf)
    
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    //sqlContext.sql("CREATE TEMPORARY TABLE SQOOP_METRICS    " +
    //           "USING org.elasticsearch.spark.sql " +
    //           "OPTIONS (resource 'index/spark') " )

    
    
    // Loop through module list
    moduleList.foreach{module => 
      val moduleDir = new File(baseDir, module)
      val files = getListOfFiles(moduleDir.getPath()).filter(! _.getPath().contains("run_sqoop_"))
                    .map{ fileWithPath =>
                      
                      val fileName = fileWithPath.getName()
                      val tableId = getTable(fileName)
                      val runDate = getRunningDate(fileName)
                                                
                      val logFile = sc.textFile(fileWithPath.toString())
                      val logDF = logFile.map { line =>  parseLogs(line)}.toDF()
    
                      logDF.registerTempTable("sqoop_metrics_tmp")
                      val metrics = sqlContext.sql("SELECT sum(mapTime) as sum_map_time, sum(gcTime) as gc_time, sum(cpuTime) as cpu_time, " + 
                             "sum(totMemory) as total_memo, sum(fileSize) as file_size, sum(transferTime) as transfer_time from sqoop_metrics_tmp")
   
                      metrics.show(false)
        
                      val metricsData = metrics.map{ p =>
                         Metrics_Sqoop(module, runDate, tableId, p.getAs[Double]("sum_map_time"), p.getAs[Double]("gc_time"), 
                             p.getAs[Double]("cpu_time"), p.getAs[Long]("total_memo"), p.getAs[Long]("file_size"), p.getAs[Double]("transfer_time") )
                      }.toDF()
                      
                                   
                      metricsData.saveToEs("spark/sqoop")   
                      
                      //sqlContext.sql("INSERT INTO TABLE SQOOP_METRICS SELECT Metrics_Sqoop(0), Metrics_Sqoop.bDate, Metrics_Sqoop.tblName, " +
                        //              "Metrics_Sqoop.mapTime, Metrics_Sqoop.gcTime, Metrics_Sqoop.cpuTime, Metrics_Sqoop.totMemory, Metrics_Sqoop.fileSize, Metrics_Sqoop.transferTime")
                   
                            
                    }
        
    }
     
    
  }
}