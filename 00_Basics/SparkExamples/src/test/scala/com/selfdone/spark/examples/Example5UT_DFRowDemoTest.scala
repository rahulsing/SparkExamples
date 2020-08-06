package com.selfdone.spark.examples


import java.sql.Date

import com.selfdone.spark.examples.Example5UT_DFRowDemo.FormatDate
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class smplRow(Timestamp: Date)
class Example5UT_DFRowDemoTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark : SparkSession = _
  @transient var df:DataFrame = _

  override def beforeAll(): Unit = {
  spark=SparkSession
  .builder()
  .appName("Test Row Type")
  .master("local[3]")
  .getOrCreate()

  df= spark.read
  .option("header","true")
  .option("inferSchema","true").csv("data\\sample.csv")


}
  override def afterAll(): Unit = {
  spark.stop()
}


  test("test Format ") {

    val df1=FormatDate(df,"Timestamp","MM/dd/yyyy")
    val rows = df1.collectAsList()


  rows.forEach(x=>println(x.get(0),x.get(1),x.get(2)))

    scala.io.StdIn.readLine()
  rows.forEach(
  //row does not have schema as collected as collection, in above step
  // column referred with index
  row=>assert(row.get(0).isInstanceOf[Date],"First Column Should be Date")
  )
}

  test("test Format Date Value") {
  val spark2=spark
  import spark2.implicits._
  // created a case class
  // create spark2 to import as implicits
  // can't use spark as it is defined as var, val type is required for import implicits
  val rows=FormatDate(df,"Timestamp","").select("Timestamp").as[smplRow]collectAsList()
  rows.forEach(
  //able to refer as "Timestamp" , by column name
  row=>assert(row.Timestamp.toString == "2014-08-27","Date for the records should be 2014-08-27")
  )
}

}