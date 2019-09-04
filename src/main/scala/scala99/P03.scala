package scala99

object P03 {

  def main(args: Array[String]): Unit = {

    def kth[T](list: List[T], k: Int): T = list(k)

    def kth1[T](list: List[T], k: Int): T = list.take(k + 1).last

    def kthRecursive[T](list: List[T], k: Int): T = list match {
      case x :: xs if k == 0 => x
      case x :: xs => kthRecursive(xs, k - 1)
      case _ => throw new NoSuchElementException
    }

    val index0 = kth(List(1, 2, 3, 4, 5), 0)
    println(index0)
    val index2 = kth1(List(1, 2, 3, 4, 5), 2)
    println(index2)
    val index4 = kthRecursive(List(1, 2, 3, 4, 5), 4)
    println(index4)
  }
}
