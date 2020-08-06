package com.selfdone.spark.examples
import org.apache.spark.sql.SparkSession

object Example7UDF  extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("Working with DF Cols").master("local[3]").getOrCreate()
    val dfSample = spark.read.option("header","true").option("inferSchema","true").csv(args(0))
    // 3 Steps
    // 1. create function
    // 2. register the function
    // 3. use the function


    //Object Expression
    import org.apache.spark.sql.functions._

    val parseGenderUDF=udf(parseGender(_:String):String)
    val sampleDF= dfSample.withColumn("Gender",parseGenderUDF(col("Gender")))

    sampleDF.show(10,false)

    //Column Expression for sql
    spark.udf.register(name="parseGenderUDF",parseGender(_:String):String)

    //can verify the registered function
    spark.catalog.listFunctions().filter(r=>r.name=="parseGenderUDF").show()
    val sampleDF2= dfSample.withColumn("Gender",expr("parseGenderUDF(Gender)"))
    sampleDF2.show(10,false)


    spark.stop()
  }

  def parseGender(s:String): String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(s.toLowerCase).nonEmpty) "Female"
    else if(malePattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }

}
