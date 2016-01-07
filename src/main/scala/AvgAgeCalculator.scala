import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/9/15.
 */
object AvgAgeCalculator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("avg age").setMaster("local")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile("C:\\sample_age_data.txt")
    val totalCount = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age=>Integer.parseInt(age))//.collect()
      .reduce((a,b)=>a+b)

    println("Total Age:" + totalAge + ";Number of People:" + totalCount )
    val avgAge : Double = totalAge.toDouble / totalCount.toDouble
    println("Average Age is " + avgAge)
  }
}
