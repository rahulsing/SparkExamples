name := "SparkESTwitter"

version := "0.1"

scalaVersion := "2.11.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"


// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
 libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.10.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
 libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

