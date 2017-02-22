package knnTesting

import knn.kNN
import org.scalatest._

object kNNTests extends FlatSpec with Matchers {

  "The knn module's distance function" should "test some stuff" in {

    kNN.xMax = 3.0
    kNN.xMax should be (3.0)
  }

}