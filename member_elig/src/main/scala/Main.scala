import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._

object Main {
  def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
      .appName("Member Eligibility")
      .master("local[*]")  // Set the master URL to run locally
      .getOrCreate()

    // Read the member_eligibility.xlsx file
    val memberEligibilityDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("./src/data/member_eligibility.xlsx")
    memberEligibilityDF.show(3)

    // Read the member_months.xlsx file
    val memberMonthsDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("./src/data/member_months.xlsx")
    memberMonthsDF.show(3)
  }
}