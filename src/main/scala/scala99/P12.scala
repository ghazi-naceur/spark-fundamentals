package scala99

object P12 {

  def main(args: Array[String]): Unit = {
    def generateDups(list: List[(Int, Any)]): List[Any] =
      list.map(kv => List.fill(kv._1)(kv._2))

    println(generateDups(List((1, 1), (4, 2), (2, 3), (2, "A"))))
  }
}
