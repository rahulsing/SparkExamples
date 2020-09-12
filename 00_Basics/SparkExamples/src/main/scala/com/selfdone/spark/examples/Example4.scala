package com.selfdone.spark.examples

import java.nio.file.Paths
import java.util.Properties
import java.util.logging.Logger

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object Example4 extends Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {


    logger.info("Started Spark Processing")

    //Get Configuration from Spark Conf
    //val spark = SparkSession.builder().config(getSparkConf).getOrCreate()

    System.setProperty("hadoop.home.dir",Paths.get(".").toAbsolutePath.toString+ "\\hadoop")

    val spark=SparkSession.builder()
      .appName("Understanding DAG")
      .master("local[*]")
      //comment and run to see the default size as 200
      .config("spark.sql.shuffle.partitions",2)
      .getOrCreate()
    val dfRawData = LoadSampleData(spark,args(0))

    //partition the inital raw file
    // job 3: new stage 1  because of repartition cause shuffling, DAG depict as Exchange and new stage 2
    val partitionedRawDataDF = dfRawData.repartition(2)

    // job 3:
    val dfCount = getCountByCountry(partitionedRawDataDF)

    // job 3: Collect ACTION
    logger.info(dfCount.collect().mkString("->"))
    scala.io.StdIn.readLine()
    logger.info("Stopped Spark Processing")
    spark.stop()
  }

  def getCountByCountry(dfSampleData: DataFrame) : DataFrame = {
    dfSampleData.where("age < 40")
      .select("Age",cols = "Gender","Country","state")
      // job 3: new stage 2 because of shuffling, DAG depict as Exchange and new stage 3
      .groupBy("Country")

      .count()
  }
  def LoadSampleData(spark: SparkSession, path:String): DataFrame = {
    spark.read
      .option("header","true")
      .option("inferSchema","true").csv(path)
  }

  def getSparkConf: SparkConf= {
   val sprkConf= new SparkConf
      val prop =new Properties()
      prop.load(Source.fromFile("spark.conf").bufferedReader())

    sprkConf
  }
}
