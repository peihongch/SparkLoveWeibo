package graphx

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig

object MongoScala {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoScala")
      .config("spark.mongodb.input.uri", "mongodb://94.191.110.118:27017")
      .getOrCreate()
    val readConfig = ReadConfig(Map("collection" -> "base_info", "database" -> "university_weibo", "uri" -> "mongodb://94.191.110.118:27017"))
    val baseInfoRdd = MongoSpark.load(spark.sparkContext, readConfig)
    val universityIdList = baseInfoRdd.map(document => {
      (document.get("id"))
    })

//    universityIdList.flatMap { universityId =>
//      val collection: String = universityId + "_weibo_info"
//      val readConfig = ReadConfig(Map("collection" -> collection, "database" -> "university_weibo", "uri" -> "mongodb://94.191.110.118:27017"))
//      val univRdd = MongoSpark.load(spark.sparkContext, readConfig)
//      univRdd.map{
//        document=>(document.get("id"), document.get(""))
//      }
//    }
  }
}