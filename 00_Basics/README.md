Refer Example 4 for a bolier plate code.

Pass argument as "data\sample.csv" for this example


Use the below as the VM Options in the Run Configurations of IntellJ
-Dlog4j.configuration=file:log4j.properties
-Dlogfile.name=hello-world-spark
-Dspark.yarn.app.container.log.dir=app-logs
