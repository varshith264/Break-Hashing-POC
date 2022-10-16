import TestHashingSpeedOnDf.findComplexHashOfRow
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, count, lit, struct, udf}

import scala.util.control.Breaks.break
import org.apache.spark.sql._

import scala.collection.mutable;

object TestHashingSpeedOnDf {
//  val csvPath = "Resources/Sales_10000_rows.csv";
  val CSV_100_ROWS_PATH = "Resources/Sales_100_rows.csv";
  val CSV_10000_ROWS_PATH = "Resources/Sales_10000_rows.csv";
  val CSV_100K_ROWS_PATH = "Resources/sampleTest.csv";
  val csvPath = CSV_100K_ROWS_PATH;

  val findComplexHashOfRow: Row => String = (row: Row) => {
    val findComplexHash = (string: String) => {
      val findHash = (string: String, p: Long, m: Long) => {
        var hash_value: Long = 0;
        var p_power: Long = 1;
        string.foreach(c => {
          hash_value = (hash_value + (c - 'a' + 1) * p_power) % m;
          p_power = (p_power * p) % m;
        })
        hash_value.toString.reverse.padTo(11, '0').reverse;
      }
      findHash(string, 257, math.pow(10,9).toLong+7) + findHash(string, 271, 2147481247) + findHash(string, 293, 998244353);
    }
    val columnNames = row.schema.fieldNames
    val Statement = mutable.StringBuilder.newBuilder
    for(index <- columnNames.indices) {
      Statement.append(columnNames(index)).append(":").append(row.get(index)).append("|");
    }
    findComplexHash(Statement.toString());
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataframe")
      .master("local[*]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");


    //first just do df.show()
    var t0 = System.nanoTime()
    readCsvAndShow(spark)
    var t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")

    //now add a new column hashcode and then do df.show()
    t0 = System.nanoTime()
    readAndGenerateHashCode(spark)
    t1 = System.nanoTime()
    println("Elapsed time while generated and adding hashcode: " + (t1 - t0) / 1000000000 + "s")

    System.in.read();
    spark.stop();
  }

  private def readCsvAndShow(spark: SparkSession): Unit = {
    val dataDf = spark.read.option("header", value = true).csv(csvPath);
    dataDf.show(100000);
  }

  def readAndGenerateHashCode(spark: SparkSession): Unit = {
    val surveyDataDf: DataFrame = spark.read.option("header", value = true).csv(csvPath);
    val findComplexHashOfRowUDF = udf(findComplexHashOfRow);
    val withHashCodeDf = surveyDataDf
      .withColumn("Hash code", findComplexHashOfRowUDF(struct(surveyDataDf.schema.fieldNames.map(col): _*)));
    withHashCodeDf.show(100000);
  }
}
