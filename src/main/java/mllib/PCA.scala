package mllib

import java.io.FileWriter

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

object PCA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("universityPCA")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("data/train2.txt").map(
      line => Vectors.dense(line.split("\\s+").map(_.toDouble))
    )

    val mat = new RowMatrix(rdd)
    val pca = mat.computePrincipalComponents(3)
    val mx = mat.multiply(pca)


//    val out = new FileWriter("data/PCA-2D.txt",false)
    mx.rows.foreach(println)
//    out.close()

  }
}
