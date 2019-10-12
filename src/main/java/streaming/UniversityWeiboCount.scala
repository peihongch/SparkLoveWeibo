package streaming

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object UniversityWeiboCount {
  val jsonPattern = "{\"time\":\"%s\", \"rankList\":[%s]}"
  val rankPattern = "{\"name\":\"%s\",\"rank\":%s}"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoScala")
      .config("spark.mongodb.input.uri", "mongodb://heiming.xyz:27017")
      .getOrCreate()

    val universityList = spark.sparkContext.textFile("docs/university_list.txt").map { line =>
      val fields = line.split(" ")
      fields(1)
    }.collect()

    val counter = Array.range(1, 109)
    val bigRDD = counter.map { c => {
      val collection = "university_heat" + c
      val readConfig = ReadConfig(Map("collection" -> collection, "database" -> "university_heat", "uri" -> "mongodb://heiming.xyz:27017"))
      MongoSpark.load(spark.sparkContext, readConfig).map(document =>
        (document.get("month").toString, universityList(c), document.getString("likeNum").toInt, document.getString("repostNum").toInt, document.getString("commentNum").toInt))
    }
    }.reduce(_ union _)

    //    bigRDD.foreach(println)

    val likePW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/like.txt")), true)
    val forwardPW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/repost.txt")), true)
    val commentPW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/comment.txt")), true)
    val totalPW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/total.txt")), true)

    val accumulatedRDD = bigRDD.groupBy(item => item._2).flatMap { g =>
      val like: AtomicInteger = new AtomicInteger(0)
      val repost: AtomicInteger = new AtomicInteger(0)
      val comment: AtomicInteger = new AtomicInteger(0)
      g._2.toList.sortBy(t => t._1).map { t =>
        val v3 = t._3 + like.get()
        like.set(v3)
        val v4 = t._4 + repost.get()
        repost.set(v4)
        val v5 = t._5 + comment.get()
        comment.set(v5)
        (t._1, t._2, v3, v4, v5)
      }.iterator
    }

    accumulatedRDD.foreach(println)

    // 按月份分组
    val groupedRDD = accumulatedRDD.groupBy(item => item._1)

    // 统计点赞数
    val likeOut = groupedRDD.map { t =>
      val valuePair = t._2.map { univ => (univ._2, univ._3) }.toList.sortBy(value => -value._2)
        .take(5) // 取前五
        .map(value => String.format(rankPattern, value._1.toString, value._2.toString)).mkString(",")
      (t._1, valuePair)
    }.sortByKey()
      //      .map(t => t._1 + "\t" + String.format(jsonPattern, t._1, t._2)).collect().mkString("\n")
      .map(t => (t._1, String.format(jsonPattern, t._1, t._2))).collectAsMap()
    //    likePW.println(likeOut)
    likeOut.foreach { entry =>
      val PW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/like/" + entry._1 + ".json")), true)
      PW.println(entry._2)
    }

    // 统计转发数
    val repostOut = groupedRDD.map { t =>
      val valuePair = t._2.map { univ => (univ._2, univ._4) }.toList.sortBy(value => -value._2)
        .take(5) // 取前五
        .map(value => String.format(rankPattern, value._1.toString, value._2.toString)).mkString(",")
      (t._1, valuePair)
    }.sortByKey()
      //      .map(t => t._1 + "\t" + String.format(jsonPattern, t._1, t._2)).collect().mkString("\n")
      .map(t => (t._1, String.format(jsonPattern, t._1, t._2))).collectAsMap()
    //    forwardPW.println(repostOut)
    repostOut.foreach { entry =>
      val PW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/repost/" + entry._1 + ".json")), true)
      PW.println(entry._2)
    }

    // 统计评论数
    val commentOut = groupedRDD.map { t =>
      val valuePair = t._2.map { univ => (univ._2, univ._5) }.toList.sortBy(value => -value._2)
        .take(5) // 取前五
        .map(value => String.format(rankPattern, value._1.toString, value._2.toString)).mkString(",")
      (t._1, valuePair)
    }.sortByKey()
      //      .map(t => t._1 + "\t" + String.format(jsonPattern, t._1, t._2)).collect().mkString("\n")
      .map(t => (t._1, String.format(jsonPattern, t._1, t._2))).collectAsMap()
    //    commentPW.println(commentOut)
    commentOut.foreach { entry =>
      val PW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/comment/" + entry._1 + ".json")), true)
      PW.println(entry._2)
    }

    // 统计热度
    val totalOut = groupedRDD.map { t =>
      val valuePair = t._2.map { univ => (univ._2, univ._3 + 3 * univ._4 + 2 * univ._5) }.toList.sortBy(value => -value._2)
        .take(5) // 取前五
        .map(value => String.format(rankPattern, value._1.toString, value._2.toString)).mkString(",")
      (t._1, valuePair)
    }.sortByKey()
      //      .map(t => t._1 + "\t" + String.format(jsonPattern, t._1, t._2)).collect().mkString("\n")
      .map(t => (t._1, String.format(jsonPattern, t._1, t._2))).collectAsMap()
    //    totalPW.println(totalOut)
    totalOut.foreach { entry =>
      val PW = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/total/" + entry._1 + ".json")), true)
      PW.println(entry._2)
    }

  }
}