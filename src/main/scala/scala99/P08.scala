package scala99

import scala.annotation.tailrec

object P08 {

  def main(args: Array[String]): Unit = {

    def eliminateConsecutiveDuplicates(list: List[Any]): List[Any] = list match {
      case Nil => Nil
      case head :: tail => head :: eliminateConsecutiveDuplicates(tail.dropWhile(element => element == head))
    }

    def eliminateConsecutiveDuplicates2(list: List[Any]): List[Any] = {
      @tailrec
      def filter(source: List[Any], noDups: List[Any], lastElement: Any): List[Any] = source match {
        case Nil => noDups
        case head :: tail if (head != lastElement) => filter(tail, noDups :+ head, head)
        case head :: tail => filter(tail, noDups, head)
      }
      filter(list, List(), null)
    }

    def eliminateConsecutiveDuplicates3(list: List[Any]): List[Any] = {
      @tailrec
      def filter(source: List[Any], noDups: List[Any]): List[Any] = source match {
        case Nil => noDups
        case head :: tail => filter(tail.dropWhile(elm => elm == head), noDups :+ head)
      }
      filter(list, List())
    }

    println(eliminateConsecutiveDuplicates(List(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 5)))
    println(eliminateConsecutiveDuplicates2(List(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 5)))
    println(eliminateConsecutiveDuplicates3(List(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 5)))
  }
}
