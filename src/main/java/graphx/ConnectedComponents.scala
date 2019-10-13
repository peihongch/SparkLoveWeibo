package graphx

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    // load university follows from mongodb
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("ConnectedComponents")
      .getOrCreate()
    val readConfig = ReadConfig(Map("collection" -> "base_info", "database" -> "university_weibo", "uri" -> "mongodb://94.191.110.118:27017"))
    val sc = spark.sparkContext
    val baseInfoRdd = MongoSpark.load(sc, readConfig)
    val universityIdList: Array[String] = baseInfoRdd.map(document => {
      document.getString("id")
    }).collect()

    val universityMap = sc.textFile("docs/university_list.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(1), fields(0))
    }.collectAsMap()
    val universityHashCode = sc.textFile("data/university_hash_code.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0), (fields(1), fields(2)))
    }.collectAsMap()
    val followsRelation = sc.textFile("data/follows_list.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0).toInt, fields(1).toInt)
    }.collect()

    val atPairList = universityIdList.flatMap { universityId =>
      val collection = universityId + "_at"
      val readConfig = ReadConfig(Map("collection" -> collection, "database" -> "university_at_relation", "uri" -> "mongodb://heiming.xyz:27017"))
      val atRdd = MongoSpark.load(sc, readConfig)
      atRdd.map { rdd =>
        ((universityId, universityMap.get(rdd.getString("university")).get), rdd.getString("atNum").toInt)
      }.collect()
    }

    val pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("at_relation_hash.txt")), true)
    atPairList.groupBy { case (id, atNum) => id }.map { group =>
      val t2 = group._2.map { case (id, atNum) => atNum }.sum
      ((group._1._1.hashCode, group._1._2.hashCode), t2)
    }
      .filter(item => item._2 > 1)
      .keys
      .foreach(hashPair => {
        pw.println(hashPair._1 + " " + hashPair._2)
        println(hashPair._1 + " " + hashPair._2)
      })
    pw.close()


    // Load the edges as a graph
//    val followsGraph = GraphLoader.edgeListFile(sc, "at_relation_hash.txt")
//    // Run PageRank
//    val comonents = followsGraph.stronglyConnectedComponents(100000000).vertices

//    val universityHashCodeTable = sc.textFile("data/university_hash_code.txt").map { line =>
//      val fields = line.split("\\s+")
//      (fields(0).toLong, (fields(1), fields(2)))
//    }

//    val ranksByUsername = universityHashCodeTable.join(comonents).map {
//      case (id, ((uid, uname), rank)) => (rank, (uid, uname))
//    }.groupByKey().map(t => {
//      t._1 + "->" + t._2.mkString(",")
//    })

//    ranksByUsername.foreach(println(_))
//    println(ranksByUsername.count())
  }
}
