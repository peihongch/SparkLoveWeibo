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

import java.util.*;

public class WeiboAtCount {
    public static final String REMOTE_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String LOCAL_MONGODB_URL = "mongodb://127.0.0.1:27017";
    public static final String DATABASE = "university_weibo";
    public static final String DEFAULT_COLLECTION = "base_info";
    private static List<String> universityIds = new ArrayList<>();

    public static void main(String[] args) throws Exception{
        Collection<Object> universityNames = ResourceUtil.getUniversityList().values();
        SparkSession sparkSession = SparkSession.builder().master("local[6]").appName("WeiboAtCount")
                .config("spark.mongodb.input.uri", REMOTE_MONGODB_URL + "/" + DATABASE + "." + DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", LOCAL_MONGODB_URL + "/sparkStreaming.result")
                /*.config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
                .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
                .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1")*/
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaMongoRDD<Document> baseInfoRDD = MongoSpark.load(jsc);
        baseInfoRDD.foreach(document -> WeiboAtCount.universityIds.add(document.getString("id")));

        //read data from mongodb
        Map<String, String> readOverrides = new HashMap<>();
        Queue<JavaRDD<Document>> universityQueue = new LinkedList<>();
        for (String universityId: universityIds) {
            readOverrides.put("collection", universityId+"_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaMongoRDD<Document> universityRDD = MongoSpark.load(jsc, readConfig);
            universityQueue.add(universityRDD);
        }

        //sparkStreaming
        JavaStreamingContext jssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), Durations.seconds(60));
        //set checkpoint for method updateStateByKey
        jssc.checkpoint("checkpoint");
        //registry accumulator for collections
        CollectionAccumulator accumulator = new CollectionAccumulator();
        jssc.ssc().sc().register(accumulator, "collectionCounter");
        //Transformation
        JavaDStream<Document> lines = jssc.queueStream(universityQueue)
                .filter(document -> !document.getList("at", String.class).isEmpty());
        JavaDStream<String> universities = lines.flatMap(document -> document.getList("at", String.class).iterator())
                .map(university -> university.substring(1))
                .filter(universityNames::contains);
        JavaPairDStream<String, Long> atCountsItem = universities.countByValue();
        Function2<List<Long>, Optional<Long>, Optional<Long>> updateFunction =
                (oldValues, state) -> {
                    Long sum = 0L;
                    if(state.isPresent()){
                        sum = state.get();
                    }
                    for(Long oldValue : oldValues){
                        sum += oldValue;
                    }
                    return Optional.of(sum);
                };
        JavaPairDStream<String, Long> atCounts = atCountsItem.updateStateByKey(updateFunction);

        //Output：write result into mongodb
        atCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) {
                accumulator.add(1);
                JavaRDD<Row> rowRDD = rdd.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple2) throws Exception {
                        return RowFactory.create(tuple2._1(), tuple2._2());
                    }
                });
                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("university", DataTypes.StringType, true)
                        , DataTypes.createStructField("num", DataTypes.LongType, true)
                });
                Dataset<Row> lines = sparkSession.createDataFrame(rowRDD, schema);
                MongoSpark.write(lines).option("collection", "at_counts_"+accumulator.value())
                        .mode(SaveMode.Overwrite).save();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}