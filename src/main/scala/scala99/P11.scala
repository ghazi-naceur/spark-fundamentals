package scala99

object P11 {

  def main(args: Array[String]): Unit = {

    def packConsecutiveDuplicates(list: List[Any]): List[List[Any]] = list match {
      case Nil => Nil
      case head :: tail => (head +: tail.takeWhile(elem => elem == head)) +: packConsecutiveDuplicates(tail.dropWhile(elem => elem == head))
    }

    def getOccurrences(list: List[Any]): List[Any] = {
      packConsecutiveDuplicates(list).map {
        case head :: Nil => head
        case lst@(head :: _) => (lst.length, head)
        case Nil => Nil
      }
    }

    println(getOccurrences(List(1, 2, 2, 2, 2, 3, 3, "A", "A")))
  }
}
