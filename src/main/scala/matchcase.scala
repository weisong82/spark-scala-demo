/**
 * Created by Administrator on 2015/9/12.
 */
object matchcase {
  def main(args: Array[String]) {
    println(fibonacci(5))
  }
  def fibonacci(in:Any):Int = in match {
    case 0 => 0
    case 1 => 1
    case n:Int =>fibonacci(n-1)+fibonacci(n-2)
    case _=>0
  }
}
