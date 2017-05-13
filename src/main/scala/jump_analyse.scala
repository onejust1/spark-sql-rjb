
import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.Random
import scala.util.control.Breaks




object jump_analyse {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("jump_analyse")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("/spark_data/visit_records.json")
    df.createOrReplaceTempView("visit")
    // Global temporary view is tied to a system preserved database `global_temp`
    var result_string = ""


    var sql = "SELECT  in_time from visit  order by `in_time`  limit 1"
    var min_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
    sql = "SELECT  in_time from visit  order by `in_time` desc  limit 1 "
    var max_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
    var now_time = 0

    var outer = new Breaks

    now_time = min_time

    while (now_time <= max_time) {
      outer.breakable {
        var jump_num = 0
        var visit_num = 0
        var time1 = now_time;
        var time2 = now_time + 300

        var sql2 = "SELECT  count(*) num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + ""; //SQL语句
        visit_num = spark.sql(sql2).collect() (0).asInstanceOf[Row].getInt(0)

        if (visit_num == 0) {
          now_time = now_time + 300;
          outer.break;
        }

        var sql3 = "SELECT  count(*) num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + " and stay_time <= 180"; //SQL语句
        jump_num = spark.sql(sql3).collect() (0).asInstanceOf[Row].getInt(0)

        var jump_rate = jump_num.asInstanceOf[Float] / visit_num.asInstanceOf[Float]
        var format_jump_rate = f"$jump_rate%1.2f"

        //每一条 jump 结果 添加到 结果集
        var jump_string =
          """{"time":""" + time1 + "," +""""jump_rate":""" + format_jump_rate + "," +""""jump_num":""" + jump_num + "," +""""visit_num":""" + visit_num + "}\n"
        result_string = result_string + jump_string


        now_time = now_time + 300;
      }

    }


    //将结果集 存入 文件
    import java.io._
    val writer = new PrintWriter(new File("\\sparkdata\\jump_rate.json"))

    writer.write(result_string)
    writer.close()


  }
}