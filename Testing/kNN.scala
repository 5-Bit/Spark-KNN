package knn;
import org.apache.spark.{SparkConf, SparkContext}

//val sConf = new SparkConf().setAppName("Spark").setMaster("Master"); // Init spark context
//val sc = new SparkContext(sConf)// Init spark context
//
//var K = sc.broadcast(5); // Or something from the command line. :/

//var x_max = sc.broadcast(irisData.map(ir => ir.x).max())
//var y_max = sc.broadcast(irisData.map(ir => ir.y).max())
//var x_min = sc.broadcast(irisData.map(ir => ir.x).min())
//var y_min = sc.broadcast(irisData.map(ir => ir.y).min())

object kNN {

  var DIM_CELLS = 10; // The number of cells in each dimension

  var xMax = 0.0
  var yMax = 0.0
  var xMin = 0.0
  var yMin = 0.0

  def cell_width(): Double = { (xMax - xMin) / (DIM_CELLS.toDouble) }
  val cell_height(): Double = { (yMax - yMin) / (DIM_CELLS.toDouble) }

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
    var cell_id = (DIM_CELLS * y_val) + x_val;
    cell_id
  }

  def pointToCellID(row:IrisPoint): Long = {
    xyToCellId(row.x, row.y)
  }

  def cellIdToPoint(cellId: Long): (Double, Double) = {
    var x_val = math.floor(cellId % DIM_CELLS)
    var y_val = math.floor(cellId / DIM_CELLS)
    (x_val, y_val)
  }
}
