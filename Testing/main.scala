import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.Map
import scala.collection.mutable.HashSet


val sConf = new SparkConf().setAppName("Spark").setMaster("local[*]"); // Init spark context
val sc = new SparkContext(sConf)// Init spark context

var K = sc.broadcast(5); // Or something from the command line. :/
var DIM_CELLS = sc.broadcast(kNN.DIM_CELLS); // The number of cells in each dimension

// TODO: Build import code
var irisData: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(x => Import.rowOfStr(x))

// This is here because Spark is yo-mama, who is an id-10-t, so bug off
var x_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).max())

var x_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).max())
var y_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).max())
var x_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).min())
var y_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).min())

kNN.xMax = x_max.value
kNN.yMax = y_max.value
kNN.xMin = x_min.value
kNN.yMin = y_min.value

// pid, x, y, class

// Assume normalized data, that will be done at some point :D
// TODO: Build custom partitioner here?
var cells = irisData.keyBy(kNN.pointToCellID).persist();

var cellCounts: Broadcast[Map[Long, Long]] = sc.broadcast(cells.countByKey());
overlapping.cellCounts = cellCounts

// value is the count here.
var fullCellCounts = cellCounts.value.filter(x => x._2 >= K.value); // [{id, count}]
var extraCellCounts = cellCounts.value.filter(x => x._2 < K.value && x._2 > 0);

// TODO: Get rid of this?
var fullCellIds = sc.broadcast(HashSet() ++ fullCellCounts.map(x => x._1));
var extraCellIds = sc.broadcast(HashSet() ++ extraCellCounts.map(x => x._1));


var fullCells = cells.filter(x => fullCellIds.value.contains(x._1)).persist();
var extraCells = cells.filter(x => extraCellIds.value.contains(x._1)).persist();


// At this point we are trained, I think, so now it's a matter of running the training set across all this?

var testRecords: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(Import.rowOfStr);

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

//// Pass to take points in empty/under-k cells, and build data
//// to make a pass over the closest cells with data, containing
//// all the data
//var cellIdsAndPidsToAddCheckTo: RDD[(Long, IrisPoint)] =
//needsAdditionalData.flatMap(x => {
//  // Get cell Ids to check to get enough data...
//  var cellID = x._1;
//  // ...
//  // Pull out the test records
//  var records = x._2._1;
//
//  var cellIds = Array[(Long, IrisPoint)]();
//  for (record <- records) {
//    var radius: Double = kNN.cell_width; // Start off with a square-unit radius
//    var center = (record.x, record.y);
//    var enclosedIdCount = cellCounts.value(cellID);
//    while (enclosedIdCount < K.value) {
//      // Expand
//      radius += 0.5 / DIM_CELLS.value;
//      // TODO: implement getCountOfOverlappedCellIds
//      enclosedIdCount = getCountOfOverlappedCellIds(radius, center);
//    }
//    // TODO: implement getOverlappedCellIds
//    for (id <- getOverlappedCellIds(radius, center)) {
//      cellIds.add((id, record));
//    }
//  }
//  cellIds
//});
//
//var knnOfUndersuppliedCells: RDD[(IrisPoint, Array[IrisPoint])] =
//  cellIdsAndPidsToAddCheckTo.keyBy(x => x._0) // Key by the cell_id
//    .join(cells) // RDD[(cell_id, Iterable[IrisPoint], Iterable[IrisPoint])]
//    .flatMap(overlapped => {
//    // var c_id = overlapped._0;
//    var test = overlapped._1;
//    var train = overlapped._2;
//    kNN.knn(K, test, train);
//  });
