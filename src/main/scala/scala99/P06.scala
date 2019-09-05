package scala99

object P06 {

  def main(args: Array[String]): Unit = {

    def palindrome[A](list: List[A]): Boolean =
      list.reverse == list

    def reverse[A](list: List[A]): List[A] = {
      def reverseInternally[A](src: List[A], dst: List[A]): List[A] = src match {
        case Nil => dst
        case head :: tail => reverseInternally(tail, head :: dst)
      }
      reverseInternally(list, List())
    }

    def recursivePalindrome[A](list: List[A]): Boolean =
      reverse(list) == list

    println(palindrome(List(1, 2, 3, 4, 5, 4, 3, 2, 1)))
    println(recursivePalindrome(List(1, 2, 3, 4, 5, 4, 3, 2, 1)))

    println(palindrome(List(1, 2, 3, 4, 5, 4, 3, 1)))
    println(recursivePalindrome(List(1, 2, 3, 4, 5, 4, 3, 1)))

  }
}
