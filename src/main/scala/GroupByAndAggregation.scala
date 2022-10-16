import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object GroupByAndAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    import spark.implicits._;

    val hockeyPlayersDF = Seq(
      ("gretzky", 40, 102, 1990),
      ("gretzky", 41, 122, 1991),
      ("gretzky", 31, 90, 1992),
      ("messier", 33, 61, 1989),
      ("messier", 45, 84, 1991),
      ("messier", 35, 72, 1992),
      ("messier", 25, 66, 1993)
    ).toDF("name", "goals", "assists", "season")

    hockeyPlayersDF.groupBy("name")
      .agg(sum("goals"))
      .show();

    hockeyPlayersDF.filter("goals >= 40 AND season >= 1991")
      .groupBy("name")
      .agg(avg("goals"), avg("assists"))
      .show()



    System.in.read();
    spark.stop();
  }
}
