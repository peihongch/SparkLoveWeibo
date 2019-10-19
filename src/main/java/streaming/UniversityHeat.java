package streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;
import util.CollectionAccumulator;
import util.ResourceUtil;

import java.math.BigInteger;
import java.util.*;

public class UniversityHeat {
    public static final String INPUT_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String INPUT_DATABASE = "university_weibo";
    public static final String INPUT_DEFAULT_COLLECTION = "base_info";
    public static final String OUTPUT_MONGODB_URL = "mongodb://heiming.xyz:27017";
    public static final String OUTPUT_DATABASE = "university_heat";
    public static final String OUTPUT_DEFAULT_COLLECTION = "heat";

    public static void main(String[] args) throws Exception{
        List<String> universityIds = ResourceUtil.getIds();

        SparkSession sparkSession = SparkSession.builder().master("local[8]").appName("UniversityHeat")
                .config("spark.mongodb.input.uri", INPUT_MONGODB_URL + "/" + INPUT_DATABASE + "." + INPUT_DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", OUTPUT_MONGODB_URL + "/" + OUTPUT_DATABASE + "." + OUTPUT_DEFAULT_COLLECTION)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        //read data from mongodb
        Map<String, String> readOverrides = new HashMap<>();
        Queue<JavaRDD<Document>> universityQueue = new LinkedList<>();
        for (String universityId: universityIds) {
            readOverrides.put("collection", universityId+"_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaMongoRDD<Document> universityRDD = MongoSpark.load(jsc, readConfig);
            JavaRDD<Document> slimUniversityRDD = universityRDD.map(document -> {
                document.remove("_id");
                document.remove("id");
                document.remove("tag");
                document.remove("at");
                document.remove("imgNum");
                document.remove("content");
                return document;
            });
            universityQueue.add(slimUniversityRDD);
        }

        //sparkStreaming
        JavaStreamingContext jssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), Durations.seconds(60));
        //set checkpoint for method updateStateByKey
        jssc.checkpoint("checkpoint");
        //registry accumulator for collections
        CollectionAccumulator accumulator = new CollectionAccumulator();
        jssc.ssc().sc().register(accumulator, "collectionCounter");
        //Transformation
        JavaDStream<Document> lines = jssc.queueStream(universityQueue);
        //accumulate likeNum, repostNum, commentNum
        JavaPairDStream<String, Map<String, BigInteger>> universityAttributes = lines.mapToPair(document -> {
            Map<String, BigInteger> attributes = new HashMap<>();
            attributes.put("likeNum", new BigInteger(document.getString("likeNum")));
            attributes.put("repostNum", new BigInteger(document.getString("repostNum")));
            attributes.put("commentNum", new BigInteger(document.getString("commentNum")));
            return new Tuple2<String, Map<String, BigInteger>>(document.getString("time").substring(0,7), attributes);
        });
        JavaPairDStream<String, Map<String, BigInteger>> accumulateAttributes = universityAttributes.reduceByKey(new Function2<Map<String, BigInteger>, Map<String, BigInteger>, Map<String, BigInteger>>() {
            @Override
            public Map<String, BigInteger> call(Map<String, BigInteger> first, Map<String, BigInteger> second) throws Exception {
                Map<String, BigInteger> attributesSum = new HashMap<>();
                attributesSum.put("likeNum", first.get("likeNum").add(second.get("likeNum")));
                attributesSum.put("repostNum", first.get("repostNum").add(second.get("repostNum")));
                attributesSum.put("commentNum", first.get("commentNum").add(second.get("commentNum")));
                return attributesSum;
            }
        });
        //Output：write result into mongodb
        accumulateAttributes.foreachRDD(new VoidFunction<JavaPairRDD<String, Map<String, BigInteger>>>() {
            @Override
            public void call(JavaPairRDD<String, Map<String, BigInteger>> rdd) {
                accumulator.add(1);
                JavaRDD<Row> rowRDD = rdd.map(new Function<Tuple2<String, Map<String, BigInteger>>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Map<String, BigInteger>> tuple2) throws Exception {
                        String month = tuple2._1();
                        Map<String, BigInteger> lrcNum = tuple2._2();
                        String likeNum = lrcNum.get("likeNum").toString();
                        String repostNum = lrcNum.get("repostNum").toString();
                        String commentNum = lrcNum.get("commentNum").toString();
                        return RowFactory.create(month, likeNum, repostNum, commentNum);
                    }
                });
                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("month", DataTypes.StringType, true)
                        , DataTypes.createStructField("likeNum", DataTypes.StringType, true)
                        , DataTypes.createStructField("repostNum", DataTypes.StringType, true)
                        , DataTypes.createStructField("commentNum", DataTypes.StringType, true)
                });
                Dataset<Row> lines = sparkSession.createDataFrame(rowRDD, schema);
                MongoSpark.write(lines).option("collection", "university_heat"+accumulator.value())
                        .mode(SaveMode.Overwrite).save();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }

}
