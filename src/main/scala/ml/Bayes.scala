package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
  * refer: http://blog.selfup.cn/683.html
  * Created by songwei on 2016/1/27.
  */
object Bayes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Estimator")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf);

    val data = sc.textFile("D:\\java\\bys.txt");
    val parsedData = data.map{line=>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
    // 把数据的60%作为训练集，40%作为测试集.
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val training =splits(0)
    val test =splits(1)

    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model =NaiveBayes.train(training,lambda = 1.0)

    //对模型进行准确度分析
    val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy-->"+accuracy)
    println("Predictionof (0.5, 3.0, 0.5):"+model.predict(Vectors.dense(0.5, 3.0, 0.5)))
    println("Predictionof (1.5, 0.4, 0.6):"+model.predict(Vectors.dense(1.5, 0.4, 0.6)))

  }
}
