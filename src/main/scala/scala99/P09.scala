package scala99

object P09 {

  def main(args: Array[String]): Unit = {

    def packConsecutiveDuplicates(list: List[Any]): List[Any] = list match {
      case head :: tail => (head +: tail.takeWhile(elem => elem == head)) +: packConsecutiveDuplicates(tail.dropWhile(elem => elem == head))
      case Nil => Nil
    }

    println(packConsecutiveDuplicates(List(1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 6, "A", "A")))
  }
}
