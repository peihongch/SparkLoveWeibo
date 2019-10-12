package graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object TriangleCount {
  def main(args: Array[String]): Unit = {
    // load university follows from mongodb
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("ConnectedComponents")
      .getOrCreate()
    val sc = spark.sparkContext

    val universityHashCode = sc.textFile("data/university_hash_code.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0), (fields(1), fields(2)))
    }.collectAsMap()

    // Load the edges as a graph
    val atGraph = GraphLoader.edgeListFile(sc, "at_relation_hash.txt")
    println("=============================================================")
    println("======================= at relation =========================")
    println("=============================================================")
    atGraph.triangleCount().vertices.sortBy(_._2,false).collect()
      .foreach(item=>println(item._2+"->"+universityHashCode.get(item._1.toString)))

    val followsGraph = GraphLoader.edgeListFile(sc, "data/follows_list.txt")
    println("=============================================================")
    println("====================== follows relation =====================")
    println("=============================================================")
    followsGraph.triangleCount().vertices.sortBy(_._2,false).collect()
      .foreach(item=>println(item._2+"->"+universityHashCode.get(item._1.toString)))
  }
}
