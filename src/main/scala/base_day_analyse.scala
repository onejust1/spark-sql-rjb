
//import com.sun.rowset.internal.Row

import scala.util.control.Breaks._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession,Row}
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.Random
import scala.util.control.Breaks




object base_day_analyse {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("base_day_analyse")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df1 = spark.read.json("hdfs://master:8020/re1/*.txt")
    df1.createOrReplaceTempView("visit")
    // Global temporary view is tied to a system preserved database `global_temp`
    var result_string = ""
    import java.io._
    val writer = new PrintWriter(new File("/root/base_day_analyse.json"))

    var sql = "SELECT  in_time from visit  order by `in_time`  limit 1"
    var min_time = spark.sql(sql).collect() (0)(0).toString.toInt
    sql = "SELECT  in_time from visit  order by `in_time` desc  limit 1 "
    var max_time = spark.sql(sql).collect() (0)(0).toString.toInt
    var now_time = 0

    var outer = new Breaks

    now_time = min_time
    var last_customer_num = 0
    var now_customer_num = 0
    var new_customer_num = 0
    var old_customer_num = 0
    var interval_customer_num = 0

    while (now_time <= max_time) {
      outer.breakable {
        var jump_num = 0
        var visit_num = 0
        var deep_in_num = 0
        var avg_stay_time = 0.0
        var time1 = now_time
        var time2 = now_time + 86400

        var sql2 = "SELECT  COUNT(DISTINCT mac) num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + " and  stay_time > 0"; //SQL语句
        interval_customer_num = spark.sql(sql2).collect() (0)(0).toString.toInt

        sql2 = "SELECT  COUNT(DISTINCT mac) num from visit   where `in_time` >= " + min_time + " and `in_time` <= " + time2 + " and  stay_time > 0"; //SQL语句
        now_customer_num = spark.sql(sql2).collect() (0)(0).toString.toInt

        new_customer_num = now_customer_num - last_customer_num
        old_customer_num = interval_customer_num - new_customer_num

        sql2 = "SELECT  count(*) jump_num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + " and  stay_time <= 180"; //SQL语句
        jump_num = spark.sql(sql2).collect() (0)(0).toString.toInt


        sql2 = "SELECT  count(*) deep_in_num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + " and  stay_time >= 1200"; //SQL语句
        deep_in_num = spark.sql(sql2).collect() (0)(0).toString.toInt


        sql2 = "SELECT  count(*) visit_num , AVG(stay_time) avg_stay_time from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + ""; //SQL语句
        var row = spark.sql(sql2).collect() (0)
        visit_num = row(0).toString.toInt
        avg_stay_time = row(1).toString.toDouble


        var jump_rate = jump_num.toDouble / visit_num.toDouble
        var deep_in_rate = deep_in_num.toDouble / visit_num.toDouble
        var format_deep_in_rate = f"$deep_in_rate%1.2f"
        var format_jump_rate = f"$jump_rate%1.2f"
        //每一条 jump 结果 添加到 结果集
        var day_string =
          """{"time":""" + time1 + "," +""""jump_out_rate":""" + format_jump_rate + "," +""""deep_in_rate":""" + format_deep_in_rate + "," +""""avg_stay_time":""" + avg_stay_time + "," +""""new_num":""" + new_customer_num + "," +""""old_num":""" + old_customer_num + "," +""""customer_num":""" + visit_num + "}\n"
        writer.write(day_string)


        now_time = now_time + 86400
        last_customer_num = now_customer_num
      }

    }


    //将结果集 存入 文件



    writer.close()


  }
}