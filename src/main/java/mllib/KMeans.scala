package mllib

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

    val rdd = sc.textFile("data/kmeans.txt").map(_.split("\\s+"))
    val LabeledPointRdd = rdd.map(x=>LabeledPoint(0,Vectors.dense(x.map(_.toDouble))))
    val dataset = spark.createDataFrame(LabeledPointRdd).select("features")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Make predictions
    val predictions = model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
