package graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponents").setMaster("local[8]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val followsGraph = GraphLoader.edgeListFile(sc, "data/follows_list.txt")
    // Run PageRank
    val comonents = followsGraph.stronglyConnectedComponents(100000000).vertices

    val universityHashCodeTable = sc.textFile("data/university_hash_code.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0).toLong, (fields(1), fields(2)))
    }

    val ranksByUsername = universityHashCodeTable.join(comonents).map {
      case (id, ((uid, uname), rank)) => (rank, (uid, uname))
    }.groupByKey().map(t=>{
      t._1+"->"+t._2.mkString(",")
    })

    ranksByUsername.foreach(println(_))
    println(ranksByUsername.count())
  }
}
