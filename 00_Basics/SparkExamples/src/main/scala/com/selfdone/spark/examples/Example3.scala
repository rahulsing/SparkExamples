package com.selfdone.spark.examples

import java.util.logging.Logger

import org.apache.spark.sql.{DataFrame, SparkSession}

object Example3 extends Serializable {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    if(args.length==0){

      System.exit(1)
    }
    logger.info("Started Spark Processing")
    val spark=SparkSession.builder()
      .master("local[3]")
      .appName("Simplified Transformation")
      //comment and run to see the default size as 200
      .config("spark.sql.shuffle.partitions",2)
      .getOrCreate()

    val dfRawData = LoadSampleData(spark,args(0))

    //partition the inital raw file
    val partitionedRawDataDF = dfRawData.repartition(2)


    val dfCount = getCountByCountry(partitionedRawDataDF)


    logger.info(dfCount.collect().mkString("->"))
    scala.io.StdIn.readLine()
    logger.info("Stopped Spark Processing")
  }

  def getCountByCountry(dfSampleData: DataFrame) : DataFrame = {
    dfSampleData.where("age < 40").select("Age",cols = "Gender","Country","state")
      .groupBy("Country")
      .count()
  }
  def LoadSampleData(spark: SparkSession, path:String): DataFrame = {
    spark.read
      .option("header","true")
      .option("inferSchema","true").csv(path)
  }

}
