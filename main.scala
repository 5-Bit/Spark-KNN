import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
// This is intended to be run in spark-shell. 
// var sc; // Init spark context
var K = sc.broadcast(5); // Or something from the command line. :/
var DIM_CELLS = sc.broadcast(10); // The number of cells in each dimension

class IrisPoint(val pid: Long, val x:Double, val y:Double, val classification: String) extends Serializable { }

class Circle (val x: Double, val y:Double, val r: Double) extends Serializable { }
class LineSegment(val x1: Double, val y1: Double, val x2: Double, val y2: Double) extends Serializable { }

object Import {
  def rowOfStr(row:String): IrisPoint = {
    val rowArr: Array[String] = row.split(",");
    new IrisPoint(rowArr(0).toLong, rowArr(1).toDouble, rowArr(2).toDouble, rowArr(5))
  }
}

// TODO: Build import code
var irisData: RDD[IrisPoint] = sc.textFile("/home/sparky/iris/1000/iris_train_pid.csv").map(Import.rowOfStr);

var x_max = sc.broadcast(irisData.map(ir => ir.x).max())
var y_max = sc.broadcast(irisData.map(ir => ir.y).max())
var x_min = sc.broadcast(irisData.map(ir => ir.x).min())
var y_min = sc.broadcast(irisData.map(ir => ir.y).min())

// See if we make it this 
object kNN {

  val xMax = x_max.value
  val yMax = y_max.value
  val xMin = x_min.value
  val yMin = y_min.value

  val cell_width = (xMax - xMin) / (DIM_CELLS.value.toDouble)
  val cell_height = (yMax - yMin) / (DIM_CELLS.value.toDouble)

  def distance(point1: IrisPoint, point2: IrisPoint): Double = {
      return math.sqrt(math.pow(math.abs(point1.x - point2.x), 2) + math.pow(math.abs(point1.y - point2.y), 2));
  }

  def knn(k: Int, test: List[IrisPoint], train: List[IrisPoint]): List[(IrisPoint, List[IrisPoint])] = {
      return test.map(testRecord => {
          var nearestNeighbors = train.sortWith((p1, p2) => distance(testRecord, p1) < distance(testRecord, p2)).take(k);
          (testRecord, nearestNeighbors)
      });
  }

  def xyToCellId(x:Double, y:Double): Long = {
    var x_val = math.floor((x - xMin) / cell_width).toInt;
    var y_val = math.floor((y - yMin) / cell_width).toInt;
    
    // Lineralize the list of cell ids 
    var cell_id = (DIM_CELLS.value * y_val) + x_val;
    cell_id
  }

  def pointToCellID(row:IrisPoint): Long = {
    xyToCellId(row.x, row.y)
  }

  def cellIdToPoint(cellId: Long): (Double, Double) = {
    var x_val = math.floor(cellId % DIM_CELLS.value)
    var y_val = math.floor(cellId / DIM_CELLS.value)
    (x_val, y_val)
  }
}

// pid, x, y, class
// How is this going to be distributed across the cluster?

// Assume normalized data, that will be done at some point :D
// TODO: Build custom partitioner here?
var cells = irisData.keyBy(kNN.pointToCellID).persist();

var cellCounts = sc.broadcast(cells.countByKey());

// value is the count here.
var fullCellCounts = cellCounts.value.filter(x => x._2 >= K.value); // [{id, count}] 
var extraCellCounts = cellCounts.value.filter(x => x._2 < K.value && x._2 > 0);

// TODO: Get rid of this?
var fullCellIds = sc.broadcast(HashSet() ++ fullCellCounts.map(x => x._1));
var extraCellIds = sc.broadcast(HashSet() ++ extraCellCounts.map(x => x._1));


var fullCells = cells.filter(x => fullCellIds.value.contains(x._1)).persist();
var extraCells = cells.filter(x => extraCellIds.value.contains(x._1)).persist();


// At this point we are trained, I think, so now it's a matter of running the training set across all this?

var testRecords: RDD[IrisPoint] = sc.textFile("/home/sparky/iris/1000/iris_train_pid.csv").map(Import.rowOfStr);

var keyedTestRecords: RDD[(Long, IrisPoint)] = testRecords.keyBy(kNN.pointToCellID);

var otherTestingStuff = keyedTestRecords.count();
// Pass to find inital KNNs, and to calculate point-eqidistant bounding geometry
var bucketedRecords: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] = keyedTestRecords.cogroup(cells).persist();
// See if we get an exception here.
var testingStuff = bucketedRecords.count()

var full = bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
    val key = cell._1;
    cellCounts.value(key) >= K.value;
});

var needsAdditionalData = bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
  val key = cell._1;
  // Fishing out the testIter
  val testIter = cell._2._1;
  cellCounts.value(key) < K.value && testIter.count(p => true) > 0; 
});

object overlapping {

  private def doesLineSegmentOverlapCircle(c: Circle, l: LineSegment): Boolean = {
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

  private def overlapAnalysis(radius: Double, center: (Double, Double)): (Long, Array[Long]) = {
    val x = center._1
    val y = center._2
    val c = Circle(x,y,radius)
    // ...
    var totalNumberOfPotentialKNNs = 0;

    for (innerXmul <- 1 to DIM_CELLS.value) {
      for (innerYmul <- 1 to DIM_CELLS.value) {
        // Corner and outer x and y
        val cx = kNN.xMin + kNN.cell_width * innerXmul
        val cy = kNN.yMin + kNN.cell_height * innerYmul
        val ox = cornerX + kNN.cell_width
        val oy = cornerY + kNN.cell_height

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
        val lineTop = LineSegment(cx, cy, ox, cy)
        val lineRight = LineSegment(ox, cy, ox, oy)
        val lineBottom = LineSegment(cx, oy, ox, oy)
        val lineLeft = LineSegment(cx, cy, cx, oy)
        val cellId = knn.xyToCellId((cx + knn.cell_width/2), cy+(knn.cell_height/2))

        if (doesLineSegmentOverlapCircle(c, lineTop) ||
          doesLineSegmentOverlapCircle(c, lineRight) ||
          doesLineSegmentOverlapCircle(c, lineBottom) ||
          doesLineSegmentOverlapCircle(c, lineLeft)) {
            totalNumberOfPotentialKNNs += cellCounts.value(cellId)
        }
      }
    }
    
    // For each cell in the grid
    //   find its corner point
    //   Find the other 3 points
    //   generate 4 lines
    //   Check to see if any of the lines
    //   of the square overlap or not.
    //   If they do, add the points associated with the cellid to the total above
    totalNumberOfPotentialKNNs 
  }

  def cellIds (radius: Double, center: (Double, Double)): Array[Long] = {
    // ...

  }
}

// Pass to take points in empty/under-k cells, and build data
// to make a pass over the closest cells with data, containing
// all the data
var cellIdsAndPidsToAddCheckTo: RDD[(Long, IrisPoint)] =
  needsAdditionalData.flatMap(x => {
    // Get cell Ids to check to get enough data...
    var cellID = x._1;
    // ...
    // Pull out the test records
    var records = x._2._1;

    var cellIds = Array[(Long, IrisPoint)]();
    for (record <- records) {
        var radius: Double = kNN.cell_width; // Start off with a square-unit radius
        var center = (record.x, record.y);
        var enclosedIdCount = cellCounts.value(cellID);
        while (enclosedIdCount < K.value) {
            // Expand
            radius += 0.5 / DIM_CELLS.value;
            // TODO: implement getCountOfOverlappedCellIds
            enclosedIdCount = getCountOfOverlappedCellIds(radius, center);
        }
        // TODO: implement getOverlappedCellIds
        for (id <- getOverlappedCellIds(radius, center)) {
          cellIds.add((id, record));
        }
    }
    cellIds
});

var knnOfUndersuppliedCells: RDD[(IrisPoint, Array[IrisPoint])] =
  cellIdsAndPidsToAddCheckTo.keyBy(x => x._0) // Key by the cell_id
  .join(cells) // RDD[(cell_id, Iterable[IrisPoint], Iterable[IrisPoint])]
  .flatMap(overlapped => {
      // var c_id = overlapped._0;
      var test = overlapped._1;
      var train = overlapped._2;
      kNN.knn(K, test, train);
  });

// :quit

// Points in empty cells
// Points in under-k cells
//
// Point in full cells
// - Cointained
// - Overlapping <-***

// mapValues keeps the same key on key/value pairs
var checkingOnFull: RDD[Array[(IrisPoint, Array[IrisPoint])], Array[(IrisPoint, Radius, Array[IrisPoint])]] = full.mapValues((testIter, trainIter) => {
    var cellId = pointToCellID(testIter.first());

    // TODO: implement cellIdToRectangloidBound
    var boundingRectanguloid = cellIdToRectangloidBound(testIter.first());
    // 
    var localNearestNeighbors = knn(K, testIter, trainIter);
    var containedKNN = Array[(IrisPoint, Array[IrisPoint])]();
    var unboundKNN = Array[(IrisPoint, Radius, Array[IrisPoint])]();
    for (center, knnList) in localNearestNeighbors {
        var boundingCircle = getBoundingCircle(center, knnList);
        if boudingRectanguloid.contains(boundingCircle) {
            containedKNN.add(center, new Radius(boundingCircle.radius), knnList.array());
        } else {
            unboundKNN.add(center, knnList.array());
        }
    }
    return (containedKNN, unboundKNN);
}).cache();

var fullKNN: RDD[(IrisPoint, Array[IrisPoint])] = checkingOnFull
  .filter(x => x._0.count > 1)
  .flatMap(x => x._0)
  .keyBy(x => x._0.pid);

// Pass and to merge them with their initial counterparts
var needsMoreChecks: RDD[(Long, IrisPoint)] = checkingOnFull
  .filter(x => x._1.count > 1)
  .flatMap(stuff => {
  var unbound = stuff._1;
  // We need to check the cell counts to figure out which cells to check

  var cellsAndPointsToCheck = Array[(Long, IrisPoint)];

  for (u <- unbound) {
      var point = u._0;
      var r = u._1;
      // TODO: implement getOverlappingCells
      var overlappingCells: Array[Long] = getOverlappingCellIds(point, r);
      for (id <- overlappingCells) {
          cellsAndPointsToCheck.add(id, point);
      }
  }
  return cellsAndPointsToCheck;
}).keyBy(_._1).persist();


// Pass to find any neighbor KNNs, 
var neighborChecked: RDD[(IrisPoint, Array[IrisPoint])] =
    needsMoreChecks // RDD[(cell_id, IrisPoint)]
    .join(cells) // RDD[(cell_id, Iterable[IrisPoint], Iterable[IrisPoint])]
    .flatMap(overlapped => {
        // var c_id = overlapped._0;
        var test = overlapped._1;
        var train = overlapped._2;
        return knn(K, test, train);
    });

// TODO: Find a way to make sure the neighborChecked and knnOfUndersuppliedCells have all of their
// IrisPoints distincted and in one place (with a reduce?)
// TODO: Output the kNN classifications from each dataset, and combine them for checking accuracy.

















