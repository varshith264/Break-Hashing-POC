import org.apache.spark.sql.SparkSession

object CreateDataFrameFromJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSON to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");

    //creating a dataframe from multiline json
    val fileName = "/home/varshith/Downloads/sample2.json";
    val jsonDf = spark.read.json(fileName);

    jsonDf.show();

    System.in.read();
    spark.stop();
  }
}
