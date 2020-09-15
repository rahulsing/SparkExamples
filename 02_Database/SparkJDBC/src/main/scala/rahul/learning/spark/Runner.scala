package rahul.learning.spark

import java.nio.file.Paths
import java.sql.DriverManager

import org.apache.spark.sql.{DataFrame, SparkSession}

object Runner extends Serializable {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir",Paths.get(".").toAbsolutePath.toString+ "\\hadoop")

    val spark=SparkSession.builder().master("local[*]").getOrCreate();

    var sfOptions = Map(
      "sfURL" -> "https://xia79632.snowflakecomputing.com/",
      "sfAccount" -> "xia79632",
      "sfUser" -> args(0),
      "sfPassword" -> args(1),
      "sfDatabase" -> "SNOWFLAKE_SAMPLE_DATA",
      "sfSchema" -> "WEATHER",
      "sfRole" -> "ACCOUNTADMIN"
    )

val queryReport1= "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1-l_discount)) as sum_disc_price, sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order   from        SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.lineitem       where   l_shipdate<=dateadd(days, -90, '1998-12-01')   group by l_returnflag, l_linestatus   order by l_returnflag, l_linestatus;"
    val df: DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      //.option("query", "select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER limit 10")
      .option("query",queryReport1)
      .load()

    df.show(false)



    spark.stop();

  }
}
