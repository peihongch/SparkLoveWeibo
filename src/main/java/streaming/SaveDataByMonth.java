package streaming;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import util.ResourceUtil;
import util.TimeUtil;

import java.io.FileNotFoundException;
import java.util.*;

public class SaveDataByMonth {
    public static final String INPUT_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String INPUT_DATABASE = "university_weibo";
    public static final String INPUT_DEFAULT_COLLECTION = "base_info";
    public static final String OUTPUT_MONGODB_URL = "mongodb://heiming.xyz:27017";
    public static final String OUTPUT_DATABASE = "month_weibo";
    public static final String OUTPUT_DEFAULT_COLLECTION = "time_period";

    public static void main(String[] args) throws FileNotFoundException {
        List<String> universityIds = ResourceUtil.getIds();

        SparkSession sparkSession = SparkSession.builder().master("local[8]").appName("SaveDataGroupByMonth")
                .config("spark.mongodb.input.uri", INPUT_MONGODB_URL + "/" + INPUT_DATABASE + "." + INPUT_DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", OUTPUT_MONGODB_URL + "/" + OUTPUT_DATABASE + "." + OUTPUT_DEFAULT_COLLECTION)
//                .config("spark.cores.max", "12")
//                .config("spark.executor.cores", "4")
//                .config("spark.executor.memory", "4g")
                .getOrCreate();
//        sparkSession.conf().set("spark.driver.cores", "4");
//        sparkSession.conf().set("spark.driver.memory", "4g");
//        sparkSession.conf().set("spark.cores.max", "12");
//        sparkSession.conf().set("spark.executor.memory", "4g");
//        sparkSession.conf().set("spark.executor.cores", "4");
//        sparkSession.conf().set("spark.default.parallelism", "800");
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        List<String> months = TimeUtil.getMonths();
        Map<String, String> readOverrides = new HashMap<>();
        Map<String, String> writeOverrides = new HashMap<>();
        for (String universityId: universityIds) {
            readOverrides.put("collection", universityId+"_weibo_info");
            ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            JavaMongoRDD<Document> universityRDD = MongoSpark.load(jsc, readConfig);

            for (String month: months) {
                JavaRDD<Document> documents = universityRDD
                        .filter(document -> document.getString("time").startsWith(month));
                writeOverrides.put("collection", month + "_weibo_info");
                WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
                MongoSpark.save(documents,writeConfig);
            }
        }

        jsc.stop();
    }
}
