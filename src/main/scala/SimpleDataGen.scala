import java.io.{File, FileWriter}
import java.util.Random

/**
 * Created by Administrator on 2015/9/15.
 */
object SimpleDataGen {
  def main(args:Array[String]) {
    val writer = new FileWriter(new File("C:\\sample_age_data.txt"),false)
    val rand = new Random()
    for ( i <- 1 to 10000000) {
      writer.write( i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
