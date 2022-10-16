import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");


    //creating a dataframe manually
//    val users = List((1, "chvars", "quantum"), (2, "chadsi", "DQ"), (3, "chokas", "CWM"), (4, "baligs", "IMD"));
//    val columnNames = Seq("Id", "Name", "Dept");
//
//    val usersRdd = spark.sparkContext.parallelize(users);
//    val usersDf1 = spark.createDataFrame(usersRdd);
//
//    usersDf1.show();
//
//    val usersDfWithColumn = usersDf1.toDF(columnNames: _*);
//
//    usersDfWithColumn.show();


    //creating a dataframe manually
    val usersDf2Data = Seq(Row(1, "chvars", "quantum"), Row(2, "chadsi", "DQ"), Row(3, "chokas", "CWM"), Row(4, "baligs", "IMD"));
    val usersDf2Data2 = Seq(Row(1, "quantum", "chvars"), Row(2, "DQ", "chadsi"), Row(3, "CWM", "chokas"), Row(4, "IMD", "baligs"));
    val usersSchema = StructType(Array(
      StructField("Id", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Dept", StringType, true)
    ));
    val usersSchema2 = StructType(Array(
      StructField("Id", IntegerType, true),
      StructField("Dept", StringType, true),
      StructField("Name", StringType, true)
    ));

    val usersDf2 = spark.createDataFrame(spark.sparkContext.parallelize(usersDf2Data), usersSchema);
    val usersDf5 = spark.createDataFrame(spark.sparkContext.parallelize(usersDf2Data2), usersSchema2);

    usersDf2.show();
    usersDf5.show();
//
//
//    //creating a dataframe while reading data from a csv file
//    val csvFileName = "/home/varshith/Downloads/business-financial-data-june-2022-quarter-csv.csv"
//    val surveyDataDf1 = spark.read.csv(csvFileName)
//
//    surveyDataDf1.show(10);
//    surveyDataDf1.printSchema();
//
//    //reads first row as headers
//    val surveyDataDf2 = spark.read.option("header", true).csv(csvFileName)
//
//    surveyDataDf2.show(10);
//    surveyDataDf2.printSchema();

    System.in.read();
    spark.stop();
  }

}
