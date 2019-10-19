package streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
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
import util.TimeUtil;

import java.math.BigInteger;
import java.util.*;

public class SparkStreaming {
    public static final String INPUT_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String INPUT_DATABASE = "month_weibo";
    public static final String INPUT_DEFAULT_COLLECTION = "base_info";
    public static final String OUTPUT_MONGODB_URL = "mongodb://heiming.xyz:27017";
    public static final String OUTPUT_DATABASE = "spark_streaming";
    public static final String OUTPUT_DEFAULT_COLLECTION = "heat";

    public static void main(String[] args) throws Exception{
        Properties nameIdPairs = ResourceUtil.getNameIdPairs();
        Set<Object> universityNames = nameIdPairs.keySet();
        List<String> months = TimeUtil.getMonths();

        SparkSession sparkSession = SparkSession.builder().master("local[8]").appName("SparkStreaming_Weibo")
                .config("spark.mongodb.input.uri", INPUT_MONGODB_URL + "/" + INPUT_DATABASE + "." + INPUT_DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", OUTPUT_MONGODB_URL + "/" + OUTPUT_DATABASE + "." + OUTPUT_DEFAULT_COLLECTION)
                /*.config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
                .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
                .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1")*/
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        //read data from mongodb
        Map<String, String> readOverrides = new HashMap<>();
        Queue<JavaRDD<Document>> universityQueue = new LinkedList<>();
        for (String month: months) {
            readOverrides.put("collection", month+"_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaMongoRDD<Document> monthRDD = MongoSpark.load(jsc, readConfig);
            JavaRDD<Document> slimMonthRDD = monthRDD.map(document -> {
                document.remove("tag");
                document.remove("imgNum");
                document.remove("time");
                document.remove("content");
                return document;
            });
            universityQueue.add(slimMonthRDD);
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
            return new Tuple2<String, Map<String, BigInteger>>(document.getString("id"), attributes);
        });
        Function2<List<Map<String, BigInteger>>
                , org.apache.spark.api.java.Optional<Map<String, BigInteger>>
                , org.apache.spark.api.java.Optional<Map<String, BigInteger>>> updateAttributes =
                (oldValues, state) -> {
                    Map<String, BigInteger> attributesSum = new HashMap<>();
                    attributesSum.put("likeNum", BigInteger.ZERO);
                    attributesSum.put("repostNum", BigInteger.ZERO);
                    attributesSum.put("commentNum", BigInteger.ZERO);
                    if(state.isPresent()){
                        attributesSum = state.get();
                    }
                    for(Map<String, BigInteger> oldValue : oldValues){
                        attributesSum.put("likeNum", attributesSum.get("likeNum").add(oldValue.get("likeNum")));
                        attributesSum.put("repostNum", attributesSum.get("repostNum").add(oldValue.get("repostNum")));
                        attributesSum.put("commentNum", attributesSum.get("commentNum").add(oldValue.get("commentNum")));
                    }
                    return Optional.of(attributesSum);
                };
        JavaPairDStream<String, Map<String, BigInteger>> accumulateAttributes = universityAttributes.updateStateByKey(updateAttributes);
        //accumulate atNum
        JavaDStream<Document> linesContainAt = lines.filter(document -> !document.getList("at", String.class).isEmpty());
        JavaDStream<String> universities = linesContainAt.flatMap(document -> document.getList("at", String.class).iterator())
                .map(university -> university.substring(1))
                .filter(universityNames::contains)
                .map(nameIdPairs::getProperty);
        JavaPairDStream<String, BigInteger> atCountsItem = universities.mapToPair(universityId -> new Tuple2<>(universityId, BigInteger.ONE));
        Function2<List<BigInteger>, org.apache.spark.api.java.Optional<BigInteger>, org.apache.spark.api.java.Optional<BigInteger>> updateAtCounts =
                (oldValues, state) -> {
                    BigInteger sum = BigInteger.ZERO;
                    if(state.isPresent()){
                        sum = state.get();
                    }
                    for(BigInteger oldValue : oldValues){
                        sum = sum.add(oldValue);
                    }
                    return Optional.of(sum);
                };
        JavaPairDStream<String, BigInteger> atCounts = atCountsItem.updateStateByKey(updateAtCounts);
        // combine likeNum, repostNum, commentNum and atNum
        JavaPairDStream<String, Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>>> results = accumulateAttributes.fullOuterJoin(atCounts);
        //Output：write result into mongodb
        results.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>>>>() {
            @Override
            public void call(JavaPairRDD<String, Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>>> rdd) {
                accumulator.add(1);
                JavaRDD<Row> rowRDD = rdd.map(new Function<Tuple2<String, Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>>>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>>> tuple2) throws Exception {
                        String id = tuple2._1();
                        Tuple2<Optional<Map<String, BigInteger>>, Optional<BigInteger>> attributes = tuple2._2();
                        Map<String, BigInteger> lrcNum = attributes._1().orElse(null);
                        String likeNum , repostNum, commentNum;
                        if (lrcNum==null) {
                            likeNum = "0";
                            repostNum="0";
                            commentNum="0";
                        } else {
                            likeNum = lrcNum.get("likeNum").toString();
                            repostNum = lrcNum.get("repostNum").toString();
                            commentNum = lrcNum.get("commentNum").toString();
                        }
                        String atNum = attributes._2().orElse(BigInteger.ZERO).toString();
                        return RowFactory.create(id, likeNum, repostNum, commentNum, atNum);
                    }
                });
                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("id", DataTypes.StringType, true)
                        , DataTypes.createStructField("likeNum", DataTypes.StringType, true)
                        , DataTypes.createStructField("repostNum", DataTypes.StringType, true)
                        , DataTypes.createStructField("commentNum", DataTypes.StringType, true)
                        , DataTypes.createStructField("atNum", DataTypes.StringType, true)
                });
                Dataset<Row> lines = sparkSession.createDataFrame(rowRDD, schema);
                MongoSpark.write(lines).option("collection", "heat_month_"+accumulator.value())
                        .mode(SaveMode.Overwrite).save();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }

}
