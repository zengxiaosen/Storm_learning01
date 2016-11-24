/**
  * Created by Administrator on 2016/11/8.
  */
object keyset_test {

  def main(args: Array[String]): Unit = {
    val scores = Map("Alice" -> 10, "Bob" -> 3, "Cindy" -> 8,"Alice" -> 10)
    print(scores.keySet)
  }
}
