package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {

    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
	Logger.getLogger("org.apache").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)
	Logger.getLogger("com").setLevel(Level.WARN)

	def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Hot Cell Analysis").getOrCreate()

    if (args.length < 1) {
      println("Usage: HotcellAnalysis <path_to_input_file>")
      spark.stop()
      return
    }

    val inputPath = args(0)
    val results = runHotcellAnalysis(spark, inputPath)
    results.show()

    spark.stop()
    }

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
	var cellCounts = spark.sql("""
	SELECT x, y, z, COUNT(*) as point_count
	FROM coordinates
	GROUP BY x, y, z
	""")
	cellCounts.createOrReplaceTempView("cell_counts")

	// Calculate sums of the points in each cell and its neighbors
	var sumCounts = spark.sql("""
	SELECT a.x, a.y, a.z,
		   SUM(b.point_count) as sum_neighbors,
		   SUM(b.point_count * b.point_count) as sum_squares
	FROM cell_counts a, cell_counts b
	WHERE ABS(a.x - b.x) <= 1 AND ABS(a.y - b.y) <= 1 AND ABS(a.z - b.z) <= 1
	GROUP BY a.x, a.y, a.z
	""")
	sumCounts.createOrReplaceTempView("sum_counts")

	// Calculate the global mean and standard deviation of the points
	val totalPoints = cellCounts.agg(sum("point_count")).first().getLong(0)
	val mean = totalPoints.toDouble / numCells // Ensure numCells is Double for floating point division
	val sumOfSquares = sumCounts.agg(sum("sum_squares")).first().getLong(0).toDouble
	val stddev = math.sqrt((sumOfSquares / numCells) - (mean * mean))

	// Compute Gi* statistic for each cell
	var giScores = spark.sql(f"""
	SELECT x, y, z,
		   ((sum_neighbors - $mean%.5f * numCells) / ($stddev%.5f * SQRT(numCells * $numCells%.5f - numCells))) as gi_star
	FROM sum_counts
	""")
	giScores.createOrReplaceTempView("gi_scores")

	val topCells = giScores.orderBy(desc("gi_star"))

	topCells
    }
}
