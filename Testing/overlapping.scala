package knn;
import org.apache.spark.broadcast.Broadcast


import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object overlapping {

  var cellCounts: Broadcast[Map[Long, Long]] = null

  // Scala port of: http://doswa.com/2009/07/13/circle-segment-intersectioncollision.html
  def closestPointOnSeg(seg_a: Vector2D, seg_b: Vector2D, circle_pos: Vector2D ): Vector2D = {
    val seg_v = seg_b - seg_a
    val pt_v = circle_pos - seg_a

    if (seg_v.len()) {
      new Vector(0,0)
    } else {
      val proj = pt_v * seg_v.unit()
      if (proj <= 0) {
        seg_a
      } else if (proj >= seg_v.len()) {
        seg_b
      } else {
        val proj_v = seg_v.unit().scalar_mul(proj)
        proj_v + seg_a
      }
    }
  }

  // Also part of the port from:
  // http://doswa.com/2009/07/13/circle-segment-intersectioncollision.html
  def segmentCircleDist(seg_a: Vector2D, seg_b: Vector2D, circ_pos: Vector2D, circ_rad: Double): Double ={
    val dist_v = circ_pos - closestPointOnSeg(seg_a, seg_b, circ_pos)
    if (dist_v.len() > circ_rad) {
      0.0
    }
    else if (dist_v.len() <= 0){
      -1.0
    } else {
      dist_v.unit().scalar_mul(circ_rad - dist_v.len()).len()
    }
  }

  // Perform the unholy regex/html parsing
  // ZALGO COMES
  def doesLineSegmentOverlapCircle(c: Circle, l: LineSegment): Boolean ={
    //segmentCircleDist(c.)
    val seg_a = new Vector2D(l.x1, l.y1)
    val seg_b = new Vector2D(l.x2, l.y2)
    val circ_pos = new Vector2D(c.x, c.y)

    segmentCircleDist(seg_a, seg_b, circ_pos, c.r) > 0
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