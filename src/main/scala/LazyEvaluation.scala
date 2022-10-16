import org.apache.spark.sql.SparkSession

object  LazyEvaluation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");


    val fileName = "/no/file/is/there/here";
    val rdd = spark.sparkContext.textFile(fileName);

    val filterRdd = rdd.filter(line => line.isEmpty);

    //error is generated only if an action is performed
    filterRdd.collect();


    System.in.read();
    spark.stop();
  }

}
