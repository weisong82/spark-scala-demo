/**
 * Created by Administrator on 2015/9/11.
 */
object kelihua {
  def add(x:Int) = (y:Int) => x + y

  def main(args: Array[String]) {
    val first = add(3)
    val second = first(2)
    println(second)

    val s = 1 to 5 toList
    val s2 = s.map(x=>x*x)
    println(s.flatMap(x=> 1 to x))
    println(s2)

  }
}
