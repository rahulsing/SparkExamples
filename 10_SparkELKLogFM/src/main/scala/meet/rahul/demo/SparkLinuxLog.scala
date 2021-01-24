package meet.rahul.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.elasticsearch.spark.rdd.EsSpark
//import org.elasticsearch.spark.sql._


object SparkLinuxLog extends Serializable {
  //[2021-01-23 01:10:14] Tasks: 199 total,   1 running, 198 sleeping,   0 stopped,   0 zombie
  case class task_cs(datetime:String,task:String,running:String,sleeping:String,stopped:String,zombie:String)
  // [2021-01-23 01:10:11]   PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND
  case class process_case(datetime:String,process_id:String,user:String,pr:String,ni:String,virt:String,res:String,srh:String,s:String,prcnt_cpu:String,prcnt_mem:String,run_time:String,command:String)
  def transform_task_log(line: String): task_cs = {
    val taskDetails:Array[String]=line.split(",")
    //taskDetails(0).split(" ")(0)
     task_cs(
      taskDetails(0).split(" ")(0)+" "+taskDetails(0).split(" ")(1).replace("[","").replace("]",""),
      taskDetails(0).split(" ")(3),
      taskDetails(1).split(" ")(3),
      taskDetails(2).split(" ")(1),
      taskDetails(3).split(" ")(3),
      taskDetails(4).split(" ")(3))
  }
  def transform_memory_log(line: String): task_cs = {
    val taskDetails:Array[String]=line.split(",")
    //taskDetails(0).split(" ")(0)
    task_cs(
      taskDetails(0).split(" ")(0),
      taskDetails(0).split(" ")(3),
      taskDetails(1).split(" ")(3),
      taskDetails(2).split(" ")(1),
      taskDetails(3).split(" ")(3),
      taskDetails(4).split(" ")(3))
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir",System.getProperty("user.dir")+"\\hadoop\\")


    //val spark=SparkSession.builder().master("local[*]").appName("Linux Log processing").getOrCreate()

     val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.es.index.auto.create", "true")
       .set("spark.es.nodes", "10.0.0.74")
       .set("spark.es.port", "9200")
       .set("es.net.http.auth.user","")
       .set("es.net.http.auth.pass", "")
       .set("es.nodes.wan.only", "false")
       .set("es.nodes.discovery", "false")
     val spark = SparkSession.builder().config(sparkConf).master("local[*]").appName("sourcedashboard").getOrCreate()


     //spark.sparkContext.wholeTextFiles("file:///C:\\Rahul\\Personal\\Learning\\BigData\\Spark\\Github\\SparkRawbase\\SparkESTwitter\\data\\top_output.log").foreach(println)

    val log_rdd=spark.sparkContext.textFile("file:///C:\\Rahul\\Personal\\Learning\\BigData\\Spark\\Github\\SparkRawbase\\SparkESTwitter\\data\\top_output.log")

    val task_rdd=log_rdd.filter(x=>x.contains("Tasks:"))
    val top_rdd=log_rdd.filter(x=>x.contains("top "))
    val cpu_rdd=log_rdd.filter(x=>x.contains("Cpu(s)"))
    val mem_rdd=log_rdd.filter(x=>x.contains("KiB Mem :"))
    val swapmem_rdd=log_rdd.filter(x=>x.contains("KiB Swap:"))
    println(task_rdd.count())
    //println(top_rdd.count())
    //println(cpu_rdd.count())
    //println(mem_rdd.count())
    //println(swapmem_rdd.count())

     // implemented transformation and case class
    //task_rdd.map(line=>transform_task_log(line)).foreach(println)

    //todo
    //mem_rdd.map(line=>transform_memory_log(line)).foreach(println)
    // else println(x.toString)


    val log_rdd_records = log_rdd.map(x => {
      val tmprow = x.replaceAll(" ", "|")
      val tmprowclean=tmprow.replaceAll("(\\|)\\1{1,}", "$1")
      (tmprowclean.split("\\|").length,tmprowclean)
    }
    )

    val process_rdd=log_rdd_records.filter(x=>x._1==14).filter(x=>x._2.contains("|PID|")==false).map(record=>record._2.split("|"))
      .map(p=>process_case(p(0),p(1),p(2),p(4),p(5),p(6),p(7),p(8),p(9),p(10),p(11),p(12),p(13)))
    //  val pmap = p.map ( line => line.split (","))
    // val personRDD = pmap.map ( p => Person (p(0), p(1), p(2). toInt))

    EsSpark.saveToEs(process_rdd, "linux/process")

  }
}

