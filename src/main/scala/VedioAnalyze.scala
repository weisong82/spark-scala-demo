import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions
import org.apache.spark.{SparkContext, SparkConf}
;

/**
 * Created by Administrator on 2015/9/16.
 *
 |-- Btype: string (nullable = true)
 |-- device: string (nullable = true)
 |-- deviceId: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- mainId: long (nullable = true)
 |-- time: string (nullable = true)
 |-- videoPart: long (nullable = true)
 |-- videoPlay: long (nullable = true) 播放时长(秒)
 */
object VedioAnalyze {

  val env = "IDC" // "DEV"  "IDC"

  var filePath: String =""
  var jdbc: String = ""


  //日期字段
  var instance: Calendar = Calendar.getInstance() //今日
  val df = new SimpleDateFormat("yyyy-MM-dd");
  val today = df.format(instance.getTime)
  instance.add(Calendar.DATE,-1)  //昨日
  val yestoday = df.format(instance.getTime)

  val df_log = new SimpleDateFormat("yyyy_MM_dd");

  if("IDC".equals(env)){
    filePath  = "file:///mnt/task/kitchen_db_api_reportvideo_"+ df_log.format(instance.getTime) + ".log"
    jdbc  = "jdbc:mysql://rdsyq4o46wne9im6f7qc.mysql.rds.aliyuncs.com:3306/zcdb?user=zhangchu&password=jGj232Sl223ff9I"
  }

  if("DEV".equals(env)){
    //for dev
    filePath = "c:/sample_vedio_data.txt"
    jdbc = "jdbc:mysql://192.168.1.230:3306/zcdb?user=kitchen&password=kitchen"
  }


  case class JDdata(time:String,ip:String,Btype:String,device:String,deviceId:String,
                    mainId:Long,videoPart:Int,videoPlay:Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("jinban-vedio-task"+today)
      .setMaster("local")
    val sc = new SparkContext(conf);
//  http://blog.csdn.net/bluejoe2000/article/details/44174091
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json(filePath);

    cleanup_sql

    //各分类下，最热播放视频100条,次数，平均时长
    val result_sum = df.groupBy("Btype","mainId").agg(functions.count(df("mainId")).as("t"),functions.avg(df("videoPlay"))).orderBy(functions.desc("t")).collect().take(100)

    result_sum.foreach(
      line=>statistics_vedio_daily_sqlInsert(line.getString(0),line.getLong(1),line.getLong(2),line.getDouble(3))
    )

//    //各类视频分别的平均播放时长,
//    val result_play_duration = df.groupBy("Btype").agg(functions.avg(df("videoPlay")).as("avg_time"))

//
//    //分类视频，取平均播放最好的50个
//    val result_good_top50 = df.groupBy("Btype","mainId").agg(functions.count(df("mainId")).as("t"),functions.avg(df("videoPlay")).as("avg_time")).orderBy(functions.desc("avg_time")).collect().take(50);
//    result_good_top50.foreach(
//      line=>statistics_vedio_daily_sqlInsert(line.getString(0),line.getLong(1),line.getLong(2),line.getDouble(3))
//    )
    //最差50
//    val result_bad_top50 = df.groupBy("Btype","mainId").agg(functions.count(df("mainId")).as("t"),functions.avg(df("videoPlay")).as("avg_time")).orderBy(functions.asc("avg_time")).collect().take(50);
//    result_bad_top50.foreach(
//      line=>statistics_vedio_daily_sqlInsert(line.getString(0),line.getLong(1),line.getLong(2),line.getDouble(3))
//    )
  }


  def cleanup_sql(): Unit ={
    val conn = DriverManager.getConnection(jdbc)
    try {
      //清空当天
      val cleanup = "delete from statistics_vedio_daily where daystamp='" +yestoday+ "'"
      conn.createStatement().executeUpdate(cleanup)

    } finally {
      conn.close
    }

  }

  def statistics_vedio_daily_sqlInsert(Btype:String,mainId:Long,count:Long,playtime:Double): Unit = {
    val conn_str = jdbc
    //classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(conn_str)
    try {
      //写入当天数据
      val prep = conn.prepareStatement("INSERT INTO zcdb.statistics_vedio_daily" +
        "(Btype,mainId,count,playtime,daystamp) VALUES (?,?,?,?,?); ")

      prep.setString(1,Btype)
      prep.setString(2,""+mainId)
      prep.setString(3,""+count)
      prep.setString(4,""+playtime.toInt)
      prep.setString(5,yestoday)
      prep.executeUpdate
    } finally {
      conn.close
    }
  }

}

