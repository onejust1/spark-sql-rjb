
import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.Random
import scala.util.control.Breaks




object come_in_analyse {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("come_in_analyse")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df1 = spark.read.json("/spark_data/visit_records.json")
    df1.createOrReplaceTempView("visit")
    val df2 = spark.read.json("/spark_data/people_flow.json")
    df2.createOrReplaceTempView("people_flow")
    // Global temporary view is tied to a system preserved database `global_temp`
    var result_string = ""


    var sql = "SELECT  in_time from visit  order by `in_time`  limit 1"
    var min_time = spark.sql(sql).collect()(0).asInstanceOf[Row].getInt(0)
    sql = "SELECT  in_time from visit  order by `in_time` desc  limit 1 "
    var max_time = spark.sql(sql).collect()(0).asInstanceOf[Row].getInt(0)
    var now_time = 0

    var outer = new Breaks

    now_time = min_time

    while (now_time <= max_time) {
      outer.breakable {
        var come_num = 0
        var time1 = now_time
        var time2 = now_time + 60

        var sql2 = "SELECT  count(*) num from visit   where `in_time` >= " + time1 + " and `in_time` <= " + time2 + ""; //SQL语句
        come_num = spark.sql(sql2).collect() (0).asInstanceOf[Row].getInt(0)

        if (come_num == 0) {
          now_time = now_time + 60
          outer.break
        }

        var sql3 = "SELECT  num from people_flow   where `time` = " + time1 + ""; //SQL语句
        var people_flow_num = spark.sql(sql3).collect() (0).asInstanceOf[Row].getInt(0)

        var in_rate = come_num.asInstanceOf[Float] / people_flow_num.asInstanceOf[Float]
        var format_in_rate = f"$in_rate%1.2f"

        //每一条 jump 结果 添加到 结果集
        var in_string =
          """{"time":""" + time1 + "," +""""num":""" + come_num + "," +""""in_rate":""" + format_in_rate + "}\n"
        result_string = result_string + in_string


        now_time = now_time + 60
      }

    }


    //将结果集 存入 文件
    import java.io._
    val writer = new PrintWriter(new File("\\sparkdata\\come_in_shop.json"))

    writer.write(result_string)
    writer.close()


  }
}