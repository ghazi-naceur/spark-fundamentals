package scala99

object P02 {

  def main(args: Array[String]): Unit = {

    def secondLast[A](list: List[A]): A = list match {
      case head :: (_ :: Nil) => head
      case _ :: tail => secondLast(tail)
      case _ => throw new NoSuchElementException("The input list must have at least 2 element")
    }

    val secLast = secondLast(List(1, 2, 11, 4, 5, 8, 10, 6))
    println(secLast)

    val secLast2 = secondLast(List())
    println(secLast2)
  }
}
