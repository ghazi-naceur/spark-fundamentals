package scala99

object P01 {

  def main(args: Array[String]): Unit = {

    def last[A](list: List[A]) : A = list match {
      case head :: Nil => head
      case _ :: tail => last(tail)
      case _ => throw new NoSuchElementException("list is empty")
    }

    val lastMember = last(List("a", "b", "c", "d"))
    println(lastMember)

    val lastMember2 = last(List())
    println(lastMember2)
  }
}
