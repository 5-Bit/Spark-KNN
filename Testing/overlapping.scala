package knn;
import org.apache.spark.broadcast.Broadcast


import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object overlapping {

  var cellCounts: Broadcast[Map[Long, Long]] = null

  def doesLineSegmentOverlapCircle(c: Circle, l: LineSegment): Boolean = {
    val dx = l.x2 - l.x1
    val dy = l.y2 - l.y1

    val A = dx * dx - dy * dy
    val B = 2 * (dx * (l.x1 - c.x) + dy * (l.y1 -c.y ))
    val C = (l.x1 - c.x) * (l.x1 - c.x) + (l.y1 - c.y) * (l.y1 - c.y) - (c.r * c.r)

    val discriminant = B * B - 4 * A * C

    if (discriminant < 0) {
      return false
    } else {
      val sqrt_disc = math.sqrt(discriminant)
      val t1 = (-B + sqrt_disc) / (2 * A)
      val t2 = (-B - sqrt_disc) / (2 * A)

      return !((0 <= t1 && t1 <= 1) || (0 <= t2 && t2 <=1))
    }
  }

  private def overlapAnalysis(radius: Double, center: (Double, Double)): (Long, ArrayBuffer[Long]) = {
    val x = center._1
    val y = center._2
    val c = new Circle(x,y,radius)
    // ...
    var totalNumberOfPotentialKNNs: Long = 0
    var overlappedCellIds: ArrayBuffer[Long] = ArrayBuffer[Long]()

    for (innerXmul <- 1 to kNN.DIM_CELLS) {
      for (innerYmul <- 1 to kNN.DIM_CELLS) {
        // Corner and outer x and y
        val cx = kNN.xMin + kNN.cell_width * innerXmul
        val cy = kNN.yMin + kNN.cell_height * innerYmul
        val ox = cx + kNN.cell_width
        val oy = cy + kNN.cell_height

        // (cx, cy)                 (ox, cy)
        /* ┏━━━━━━━━━━━━━━━━━━━━━━━━━┓
         * ┃                         ┃
         * ┃                         ┃
         * ┃                         ┃
         * ┃                         ┃
         * ┃                         ┃
         * ┗━━━━━━━━━━━━━━━━━━━━━━━━━┛
         * (cx, oy)                 (ox, oy)
         *
         */
        val lineTop = new LineSegment(cx, cy, ox, cy)
        val lineRight = new LineSegment(ox, cy, ox, oy)
        val lineBottom = new LineSegment(cx, oy, ox, oy)
        val lineLeft = new LineSegment(cx, cy, cx, oy)
        val cellId: Long = kNN.xyToCellId((cx + kNN.cell_width/2), cy+(kNN.cell_height/2))

        if (doesLineSegmentOverlapCircle(c, lineTop) ||
          doesLineSegmentOverlapCircle(c, lineRight) ||
          doesLineSegmentOverlapCircle(c, lineBottom) ||
          doesLineSegmentOverlapCircle(c, lineLeft)) {
          totalNumberOfPotentialKNNs += cellCounts.value(cellId)
          overlappedCellIds.append(cellId)

        }
      }
    }

    (totalNumberOfPotentialKNNs, overlappedCellIds)
  }

}