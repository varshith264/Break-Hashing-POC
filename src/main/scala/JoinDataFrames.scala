import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object JoinDataFrames {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    import spark.implicits._;


    val sc = spark.sparkContext;

    val kids = sc.parallelize(List(
      Row(40, "Mary", 1),
      Row(41, "Jane", 3),
      Row(42, "David", 3),
      Row(43, "Angela", 2),
      Row(44, "Charlie", 1),
      Row(45, "Jimmy", 2),
      Row(46, "Lonely", 7)
    ));

    val kidsSchema = StructType(Array(
      StructField("Id", IntegerType),
      StructField("Name", StringType),
      StructField("Team", IntegerType)
    ));

    val kidsDF = spark.createDataFrame(kids, kidsSchema)
    kidsDF.show();

    val teams = sc.parallelize(List(
      Row(1, "The Invincibles"),
      Row(2, "Dog Lovers"),
      Row(3, "Rockstars"),
      Row(4, "The Non-Existent Team")
    ));

    val teamsSchema = StructType(Array(
      StructField("TeamId", IntegerType),
      StructField("TeamName", StringType)
    ));

    val teamsDF = spark.createDataFrame(teams, teamsSchema);
    teamsDF.show();


    //Inner join
    val joinCondition = kidsDF.col("Team") === teamsDF.col("TeamId")
    val kidsTeamsDF1 = kidsDF.join(teamsDF, joinCondition, "inner")
      .select("Id", "Name", "Team", "TeamName");
    kidsTeamsDF1.show();

    val kidsTeamsDF2 = kidsDF.join(teamsDF, joinCondition, "left_outer")
    kidsTeamsDF2.show();

    val kidsTeamsDF3 = kidsDF.join(teamsDF, joinCondition, "right_outer")
    kidsTeamsDF3.show();

    val kidsTeamsDF4 = kidsDF.join(teamsDF, joinCondition, "outer")
    kidsTeamsDF4.show();



    System.in.read();
    spark.stop();
  }
}
