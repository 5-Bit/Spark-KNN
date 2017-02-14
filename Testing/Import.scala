object Import {
  def rowOfStr(row:String): IrisPoint = {
    val rowArr: Array[String] = row.split(",");
    new IrisPoint(rowArr(0).toLong, rowArr(1).toDouble, rowArr(2).toDouble, rowArr(5))
  }
}
