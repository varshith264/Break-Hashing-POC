import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreateDataFrameFromCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");


    //creating a dataframe while reading data from a csv file
    val csvFileName = "/home/varshith/Downloads/business-financial-data-june-2022-quarter-csv.csv"
    val surveyDataDf1 = spark.read.csv(csvFileName)

    surveyDataDf1.show(10);
    surveyDataDf1.printSchema();


    //reads first row as headers
    val surveyDataDf2 = spark.read.option("header", true).csv(csvFileName)

    surveyDataDf2.show(10);
    surveyDataDf2.printSchema();
    //if you want you can provide schema as an option before hand, otherwise spark will read every column as string

    System.in.read();
    spark.stop();
  }

}
