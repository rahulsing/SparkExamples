package com.selfdone.spark.examples

import org.apache.spark.sql.SparkSession

object Example8Misc extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[100]").appName("").getOrCreate()
    val dfSample = spark.read.option("header","true").option("inferSchema","true").csv(args(0))
    // https://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/functions$.html
    import org.apache.spark.sql.functions._
    dfSample.agg(skewness(col("Gender"))).show()
    scala.io.StdIn.readLine()
    spark.stop()
  }
}
