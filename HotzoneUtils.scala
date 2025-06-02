package cse511

object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rectangle = queryRectangle.split(",").map(_.toDouble)
    val point = pointString.split(",").map(_.toDouble)

    if (rectangle.length != 4 || point.length != 2) return false

    val x1 = rectangle(0)
    val y1 = rectangle(1)
    val x2 = rectangle(2)
    val y2 = rectangle(3)

    val px = point(0)
    val py = point(1)

    (px >= x1 && px <= x2) && (py >= y1 && py <= y2)
  }
}
