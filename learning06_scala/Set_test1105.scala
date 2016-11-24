/**
  * Created by Administrator on 2016/11/5.
  */
object Set_test1105 {
  def main(args: Array[String]): Unit = {
     println("==="+digits(55)+"===")

    List(1,7,2,9).foldLeft("")(_+_).foreach(println)
    (0 /: List(1,7,2,9))(_-_)
    val freq = scala.collection.mutable.Map[Char, Int]()
    for(c <- "Mississippi") freq(c) = freq.getOrElse(c, 0) + 1
    //freq.foreach(println)
    val freq1 = Map[Char, Int]()
    (freq1 /: "Mississippi"){
      (m, c) => m + (c -> (m.getOrElse(c, 0) + 1))
    }
    freq1.foreach(println)
    //(1 to 10).scanLeft(0)(_+_).foreach(println)
    val prices = List(5.0, 20.0, 9.95)
    val quantities = List(10, 2, 1)
    val hehe = (prices zip quantities) map {
      p => p._1 * p._2
    }
    println(hehe + " ")

    val hehe1 = "Scala".zipWithIndex
    println(hehe1)
    val hehe2 = "Scala".zipWithIndex.max
    println(hehe2)
    prices.par.sum //并发求和
    for(i <- (0 until 100).par) yield  i + " "
  }
  def digits(n: Int) : Set[Int] = {
    if (n < 0) digits(-n)
    else if (n < 10) Set(n)
    else digits(n / 10) + (n % 10)
  }
}
