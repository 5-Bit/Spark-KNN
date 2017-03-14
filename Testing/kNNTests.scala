package knnTesting

import org.scalatest._

class kNNTests extends FlatSpec with Matchers {

  "The knn module's distance function" should "test some stuff" in {
    knn.kNN.xMax = 3.0
    knn.kNN.xMax should be (3.0)
  }

}