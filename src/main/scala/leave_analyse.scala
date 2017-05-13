
import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.Random
import scala.util.control.Breaks




object leave_analyse {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("leave_analyse")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("/spark_data/visit_records.json")
    df.createOrReplaceTempView("visit")
    // Global temporary view is tied to a system preserved database `global_temp`
    var result_string = ""


    var sql = "SELECT  out_time from visit  order by `in_time`  limit 1"
    var min_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
    sql = "SELECT  out_time from visit  order by `in_time` desc  limit 1 "
    var max_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
    var now_time = 0

    var outer = new Breaks

    now_time = min_time

    while (now_time <= max_time) {
      outer.breakable {
        var leave_num = 0
        var time1 = now_time
        var time2 = now_time + 300

        var sql2 = "SELECT  count(*) num from visit   where `out_time` >= " + time1 + " and `out_time` <= " + time2 + ""; //SQL语句
        leave_num = spark.sql(sql2).collect() (0).asInstanceOf[Row].getInt(0)

        if (leave_num == 0) {
          now_time = now_time + 300
          outer.break
        }


        //每一条 jump 结果 添加到 结果集
        var leave_string =
          """{"time":""" + time1 + "," +""""num":""" + leave_num + "}\n"
        result_string = result_string + leave_string


        now_time = now_time + 300
      }

    }


    //将结果集 存入 文件
    import java.io._
    val writer = new PrintWriter(new File("\\sparkdata\\leave_num.json"))

    writer.write(result_string)
    writer.close()


  }
}