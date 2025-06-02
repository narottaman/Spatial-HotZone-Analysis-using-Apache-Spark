package cse511

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    coordinateOffset match {
      case 0 => Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => Math.floor((inputString.split(",")(1).replace(")", "").toDouble / coordinateStep)).toInt
      case 2 => {
        val timestamp = timestampParser(inputString)
        dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    new Timestamp(parsedDate.getTime)
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  // Calculate spatial weight between two cells
  def calculateSpatialWeight(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Int = {
    if (Math.abs(x1 - x2) <= 1 && Math.abs(y1 - y2) <= 1 && Math.abs(z1 - z2) <= 1) 1 else 0
  }
}
