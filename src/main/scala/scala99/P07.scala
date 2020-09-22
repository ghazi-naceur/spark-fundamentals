package scala99

object P07 {

  def main(args: Array[String]): Unit = {

    def flatten(list: List[Any]): List[Any] = {
      var flattened = List[Any]()
      for (element <- list) {
        element match {
          case element: List[Any] => flattened = flattened ++ flatten(element)
          case _ => flattened = flattened :+ element
        }
      }
      flattened
    }

    println(flatten(List(List(1, 2, 3), List(4, List(5, List(6, List(7)))))))
  }
}
