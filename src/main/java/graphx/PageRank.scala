package graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[8]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val followsGraph = GraphLoader.edgeListFile(sc, "data/follows_list.txt")
    // Run PageRank
    val ranks = followsGraph.pageRank(0.000000000001).vertices

    println(ranks.first())

    val universityHashCodeTable = sc.textFile("data/university_hash_code.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0).toLong, (fields(1), fields(2)))
    }

    println(universityHashCodeTable.first())

    val ranksByUsername = universityHashCodeTable.join(ranks).map {
      case (id, ((uid, uname), rank)) => (rank, (uid, uname))
    }

    val rankedRDD = ranksByUsername.sortByKey()
    println(rankedRDD.collect().mkString("\n"))
    println(rankedRDD.count())
  }
}
