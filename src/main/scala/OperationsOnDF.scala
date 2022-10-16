import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType,StringType, StructField, StructType}

object OperationsOnDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");


    //creating a dataframe while reading data from a csv file
    val csvFileName = "/home/varshith/Downloads/business-financial-data-june-2022-quarter-csv.csv";
    val schema = StructType(Array(
      StructField("Series_reference", StringType, true),
      StructField("Period", StringType, true),
      StructField("Data_value", DoubleType, true),
      StructField("Suppressed", StringType, true),
      StructField("STATUS", StringType, true),
      StructField("UNITS", StringType, true),
      StructField("Magnitude", IntegerType, true),
      StructField("Subject", StringType, true),
      StructField("Group", StringType, true),
      StructField("Series_title_1", StringType, true),
      StructField("Series_title_2", StringType, true),
      StructField("Series_title_3", StringType, true),
      StructField("Series_title_4", StringType, true),
      StructField("Series_title_5", StringType, true)
    ));

    //reads first row as headers
    val csvDf = spark.read.option("header", true).schema(schema).csv(csvFileName)

    csvDf.show(10);
    //caching to check the execution time in spark UI
    csvDf.cache();
    //if you want you can provide schema as an option before hand, otherwise spark will read every column as string
    csvDf.printSchema();

    //operations using filter
    csvDf.filter("Data_value==1233.7").show(truncate = false);
    csvDf.filter(String.format("STATUS==%s", "F")).show(truncate = false);

    csvDf.filter("Data_value==1233.7").show(truncate = false);
    csvDf.filter(csvDf("Magnitude") === 6).limit(5).show(truncate = false);

    csvDf.select("*").groupBy("Group", "STATUS").count().show();

    System.in.read();
    spark.stop();
  }

}
