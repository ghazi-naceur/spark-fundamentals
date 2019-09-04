package scala99

object P05 {

  def main(args: Array[String]): Unit = {

    def reverse[A](list: List[A]): List[A] = {
      def reverseInternally[A](src: List[A], dst: List[A]): List[A] = src match {
        case Nil => dst
        case head :: tail => reverseInternally(tail, head :: dst)
      }
      reverseInternally(list, List())
    }

    val reversed = reverse(List(1,2,3,4,5))
    println(reversed)

    val reversed2 = reverse(List())
    println(reversed2)
  }
}
