package com.selfdone.spark.examples

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.functions._


object Example6DFColumn  extends Serializable {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Working with DF Cols").master("local[3]").getOrCreate()
    val dfSample = spark.read.option("header","true").option("inferSchema","true").csv(args(0))

    //simplest way to refer as string column name.
    // string column names , can't be used with other methods like col or $
    dfSample.select("Age","Gender")


    //can refer columns with col for column object
    dfSample.select(col("Country"),column("state") ).show()

    //below are scala shorthands to refer
    // Need to import implicits
    import spark.implicits._
    dfSample.select($"Country",$"Gender",'Gender ).show()
    //can be used with col function
    dfSample.select(col("Age"),$"Country",$"Gender",'Gender ).show()


    // ############ Column Expression : 1. String Express or 2. Column object expression
    // Can't use string column when using expr, to use string column use selectExpr
    // can use any other like col, column, $, ' and call any spark sql functions

    dfSample.select(col("Age"),expr("no_employees"),$"Gender",expr("concat(state,'-',Country)")).show()
    // most commonly used
    // when using expression
    dfSample.selectExpr("Age","no_employees","Gender","concat(state,'-',Country)").show()


    dfSample.select($"Age",$"no_employees",$"Gender",concat($"state",$"Country")).show()


    //dfSample.show()
    spark.stop()
  }
}

