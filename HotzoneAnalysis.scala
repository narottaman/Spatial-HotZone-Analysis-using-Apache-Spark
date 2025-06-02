package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hot Zone Analysis")
      .getOrCreate()

    if (args.length < 2) {
      println("Usage: HotzoneAnalysis <point_data_path> <rectangle_data_path>")
      spark.stop()
      return
    }

    val pointPath = args(0)
    val rectanglePath = args(1)
    val results = runHotZoneAnalysis(spark, pointPath, rectanglePath)

    results.show()

    spark.stop()
  }

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    // Load points data
    var pointDf = spark.read.format("csv").option("delimiter", ";").option("header", "false").load(pointPath)
    spark.udf.register("trim", (s: String) => s.replace("(", "").replace(")", ""))
    pointDf = pointDf.selectExpr("trim(_c5) as location")
    pointDf.createOrReplaceTempView("point")

    // Load rectangles data
    val rectangleDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(rectanglePath)
    rectangleDf.createOrReplaceTempView("rectangle")

    // Register ST_Contains UDF
    spark.udf.register("ST_Contains", (rect: String, point: String) => HotzoneUtils.ST_Contains(rect, point))

    // Run the range join and count how many points are in each rectangle
    val resultDf = spark.sql("""
      SELECT rectangle._c0 AS rectangle, COUNT(*) AS point_count
      FROM rectangle, point
      WHERE ST_Contains(rectangle._c0, point.location)
      GROUP BY rectangle._c0
      ORDER BY rectangle._c0
    """)

    resultDf
  }
}
