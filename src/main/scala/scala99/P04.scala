package scala99

object P04 {

  def main(args: Array[String]): Unit = {

    def length[A](list: List[A]): Int = {
      def acc[A](list: List[A], inc: Int): Int = list match {
        case Nil => inc
        case _ :: tail => acc(tail, inc + 1)
      }
      acc(list, 0)
    }

    val size1 = length(List(1, 2, 3, 4, 5))
    println(size1)

    val size2 = length(List())
    println(size2)
  }
}
