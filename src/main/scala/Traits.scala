/**
 * raits就像是有函数体的Interface。使用with关键字来混入。

这个例子是给java.util.ArrayList添加了foreach的功能。
 * Created by Administrator on 2015/9/11.
 */
object Traits {

  def main(args: Array[String]) {
    val list = new java.util.ArrayList[Int]() with ForEachAble[Int]
    list.add(1); list.add(2)

    println("For each: "); list.foreach(x => println(x))
    //println("Json: " + list.toJson())
  }
  trait ForEachAble[A] {
    def iterator: java.util.Iterator[A]
    def foreach(f: A => Unit) = {
      val iter = iterator
      while (iter.hasNext)
        f(iter.next)
    }
  }

//  trait JsonAble {
//    def toJson() =
//      scala.util.parsing.json.JSONFormat.defaultFormatter(this)
//  }



}
