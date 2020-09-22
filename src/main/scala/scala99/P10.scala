package scala99

object P10 {

  def main(args: Array[String]): Unit = {

    def packConsecutiveDuplicates(list: List[Any]): List[List[Any]] = list match {
      case Nil => Nil
      case head :: tail => (head +: tail.takeWhile(elem => elem == head)) +: packConsecutiveDuplicates(tail.dropWhile(elem => elem == head))
    }

    def getOccurrences(list: List[Any]): List[(Int, Any)] =
      packConsecutiveDuplicates(list).map((lst: List[Any]) => (lst.size, lst.head))

    println(getOccurrences(List(1, 2, 2, 2, 2, 3, 3, "A", "A")))
  }
}
