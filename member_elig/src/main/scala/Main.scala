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

    // Read the member_months.xlsx file
    val memberMonthsDF = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("./src/data/member_months.xlsx")

    //Join on member_id
    val joinedDF =  memberEligibilityDF.join(memberMonthsDF, "member_id")
    //joinedDF.show(3)

    //Aggregate for total months
    val totalMemberMonthsDF = joinedDF
      .groupBy("member_id", "first_name", "middle_name", "last_name")
      .agg(count("eligibility_member_month").alias("total_member_months"))
    ////totalMemberMonthsDF.show(3)

    //Write result to json file
    totalMemberMonthsDF.write.json("out/total_member_months.json")

    //Grouping by member and year
    val memberMonthsPerYearDF = joinedDF
      .withColumn("year", year(col("eligiblity_effective_date")))
      .groupBy("member_id", "year")
      .agg(count("eligibility_member_month").alias("total_member_months"))
    //memberMonthsPerYearDF.show(3)

    //Write result to json file
    memberMonthsPerYearDF.write.json("out/member_months_per_year.json")
  }
}