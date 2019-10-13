package streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.bson.Document;
import scala.Tuple2;
import util.ResourceUtil;

import java.math.BigInteger;
import java.util.*;

public class UniversityAtCount {
    public static final String INPUT_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String INPUT_DATABASE = "university_weibo";
    public static final String INPUT_DEFAULT_COLLECTION = "base_info";
    public static final String OUTPUT_MONGODB_URL = "mongodb://heiming.xyz:27017";
    public static final String OUTPUT_DATABASE = "university_at_relation";
    public static final String OUTPUT_DEFAULT_COLLECTION = "at_counts";
    private static final String template = "{\"university\":\"%s\",\"atNum\":\"%s\"}";
    private static List<String> universityIds = new ArrayList<>();
    private static Collection<Object> universityNames;


    public static void main(String[] args) throws Exception{
        universityNames = ResourceUtil.getUniversityList().values();
        SparkSession sparkSession = SparkSession.builder().master("local[8]").appName("UniversityAtCount")
                .config("spark.mongodb.input.uri", INPUT_MONGODB_URL + "/" + INPUT_DATABASE + "." + INPUT_DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", OUTPUT_MONGODB_URL + "/" + OUTPUT_DATABASE + "." + OUTPUT_DEFAULT_COLLECTION)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaMongoRDD<Document> baseInfoRDD = MongoSpark.load(jsc);
        baseInfoRDD.foreach(document -> universityIds.add(document.getString("id")));

        Map<String, String> readOverrides = new HashMap<>();
        Map<String, String> writeOverrides = new HashMap<>();
        for (String universityId: universityIds) {
            readOverrides.put("collection", universityId+"_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaMongoRDD<Document> universityRDD = MongoSpark.load(jsc, readConfig);
            JavaPairRDD<String, BigInteger> atCounts = universityRDD
                    .filter(document -> !document.getList("at", String.class).isEmpty())
                    .flatMap(document -> document.getList("at", String.class).iterator())
                    .map(university -> university.substring(1))
                    .filter(item->universityNames.contains(item))
                    .mapToPair(universityName -> new Tuple2<>(universityName, BigInteger.ONE))
                    .reduceByKey(BigInteger::add);
            JavaRDD<Document> results = atCounts
                    .map(pair -> Document.parse(String.format(template, pair._1(), pair._2().toString())));
            writeOverrides.put("collection", universityId+"_at");
            WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
            MongoSpark.save(results, writeConfig);
        }

        jsc.stop();
    }
}
