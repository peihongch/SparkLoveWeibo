package graphx

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object OutDegree {
  val jsonPattern = "{\"rankList\":[%s]}"
  val rankPattern = "{\"name\":\"%s\",\"rank\":%s}"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("ConnectedComponents")
      .getOrCreate()
    val sc = spark.sparkContext

    val universityHashCode = sc.textFile("data/university_hash_code.txt").map { line =>
      val fields = line.split("\\s+")
      (fields(0), fields(2))
    }.collectAsMap()

    // Load the edges as a graph
    val atGraph = GraphLoader.edgeListFile(sc, "data/at_relation_hash.txt")
    val atResult = atGraph.outDegrees.map { case (id, degree) => (degree, universityHashCode(id.toString)) }
      .sortBy { case (degree, name) => -degree }
      .map{ case (degree, name) => String.format(rankPattern, name, degree.toString)}
      .collect().mkString(",")
    val at_pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/graphX/at_outDegree.json")), true)
    at_pw.println(String.format(jsonPattern, atResult))
    at_pw.close()

    val followsGraph = GraphLoader.edgeListFile(sc, "data/follows_list.txt")
    val followResult = followsGraph.outDegrees.map { case (id, degree) => (degree, universityHashCode(id.toString)) }
      .sortBy { case (degree, name) => -degree }
      .map{ case (degree, name) => String.format(rankPattern, name, degree.toString)}
      .collect().mkString(",")
    val pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("output/graphX/follows_outDegree.json")), true)
    pw.println(String.format(jsonPattern, followResult))
    pw.close()
  }
}
