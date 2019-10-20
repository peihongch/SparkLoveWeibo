package mllib

import java.io.FileWriter

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object KMeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("universityKMeans")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("data/train2.txt").map(_.split("\\s+"))
    val LabeledPointRdd = rdd.map(x=>LabeledPoint(0,Vectors.dense(x.map(_.toDouble))))
    val dataset = spark.createDataFrame(LabeledPointRdd).select("features")

//    for (a <- 2 until 10) {
//      for (b <- 1 until 60) {
//        val out = new FileWriter("data/compare2.txt",true)
//        val kmeans = new KMeans().setK(a).setSeed(b.toLong)
//        val model = kmeans.fit(dataset)
//        val predictions = model.transform(dataset)
//        val evaluator = new ClusteringEvaluator()
//        val silhouette = evaluator.evaluate(predictions)
//        out.write(s"$a $b $silhouette" + "\n")
//        out.close()
//      }
//    }

    val kmeans = new KMeans().setK(6).setSeed(21L)
    val model = kmeans.fit(dataset)

    val predictions = model.transform(dataset)

    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val out = new FileWriter("data/predict6.txt",false)
    predictions.collect().foreach(row => {
      out.write(String.valueOf(row(1))+"\n")
    })
    out.close()
  }
}
