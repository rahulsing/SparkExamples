package com.selfdone.spark.examples

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}


//Cntrl + Shift + T
object Example5UT_DFRowDemo extends Serializable {
  def main(args: Array[String]): Unit = {


    val spark=SparkSession.builder().appName("Spark Unit Testing Demo")
      .master("local[3]")
      //.config("spark.sql.shuffle.partitions",2)
      .getOrCreate()
    val dfRawData = LoadSampleData(spark,args(0))
    //import spark.sqlContext.implicits._

    val dfFormated=FormatDate(dfRawData,"Timestamp","MM/dd/yyyy")


    dfFormated.show()
    dfFormated.printSchema()
    //scala.io.StdIn.readLine()
  }
  def FormatDate(df:DataFrame, clmn:String, frmt:String): DataFrame ={

   val df1= df.withColumn(clmn,date_format(col(clmn),frmt)).withColumn(clmn,to_date(col(clmn)))
    df1
  }
  def LoadSampleData(spark: SparkSession, path:String): DataFrame = {
    spark.read
      .option("header","true")
      .option("inferSchema","true").csv(path)
  }
}
