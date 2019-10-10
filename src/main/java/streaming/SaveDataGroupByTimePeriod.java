package streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import util.TimeUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SaveDataGroupByTimePeriod {
    public static final String REMOTE_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String OUTPUT_MONGODB_URL = "mongodb://heiming.xyz:27017";
    public static final String INPUT_DATABASE = "university_weibo";
    public static final String OUTPUT_DATABASE = "group_by_time_period";
    public static final String DEFAULT_INPUT_COLLECTION = "base_info";
    public static final String DEFAULT_OUTPUT_COLLECTION = "time_period";
    private static List<String> universityIds = new ArrayList<>();
    private static Map<String, String> readOverrides = new HashMap<>();
    private static final String template = "{\"id\":\"%s\",\"forward\":\"%s\",\"likeNum\":\"%s\",\"forwardNum\":\"%s\",\"commentNum\":\"%s\"}";


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[8]").appName("SaveDataGroupByTimePeriod")
                .config("spark.mongodb.input.uri", REMOTE_MONGODB_URL + "/" + INPUT_DATABASE + "." + DEFAULT_INPUT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", OUTPUT_MONGODB_URL + "/" + OUTPUT_DATABASE + "." + DEFAULT_OUTPUT_COLLECTION)
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaMongoRDD<Document> baseInfoRDD = MongoSpark.load(jsc);
        baseInfoRDD.foreach(document -> SaveDataGroupByTimePeriod.universityIds.add(document.getString("id")));

        AtomicInteger counter = new AtomicInteger(1);
        universityIds.forEach(universityId -> {
            System.out.println("Current University: " + universityId);
            System.out.println("==================" + counter.get() + "==================");
            counter.incrementAndGet();
            readOverrides.put("collection", universityId + "_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaRDD<Document> rdd = MongoSpark.load(jsc, readConfig);

            for (int i = 2013; i < 2020; i++) {
                for (int j = 1; j < 12; j += 3) {
                    final int year = i, month = j;
                    JavaRDD<Document> filteredRDD = rdd.filter(row -> {
                        String time = (String) row.get("time");
                        Date date = TimeUtil.parseDate(time);
                        return date != null && TimeUtil.isBetween(date, year, year + 1, month);
                    });
                    JavaRDD<Document> finalRDD = filteredRDD.map(document ->
                            Document.parse(String.format(template,
                                    document.get("id"),
                                    document.get("repost"),
//                            document.get("at"),
                                    document.get("likeNum"),
                                    document.get("repostNum"),
                                    document.get("commentNum")
                                    )
                            ));
                    Map<String, String> writeOverrides = new HashMap<>();
                    writeOverrides.put("collection", year + "_" + month);
                    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
                    MongoSpark.save(finalRDD,writeConfig);
                }
            }
        });
    }
}
