package util;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import entity.BaseInfo;
import entity.WeiboItem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

public final class MongoUtil {
    public static final String MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String DATABASE = "university_weibo";
    public static final String DEFAULT_COLLECTION = "base_info";
    private SparkSession sparkSession;
    private JavaSparkContext jsc;
    private Map<String, String> readOverrides;

    private MongoUtil() {
        sparkSession = SparkSession.builder().master("local").appName("ReadFromMongodb")
                .config("spark.mongodb.input.uri", MONGODB_URL + "/" + DATABASE + "." + DEFAULT_COLLECTION)
                .getOrCreate();
        jsc = new JavaSparkContext(sparkSession.sparkContext());
        readOverrides = new HashMap<>();
    }

    public static MongoUtil getMongoUtil() {
        return new MongoUtil();
    }

    private JavaMongoRDD<Document> getWeiboRDD(String collection) {
        readOverrides.put("collection", collection);
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        return MongoSpark.load(jsc, readConfig);
    }

    public JavaMongoRDD<Document> getBaseInfoRDD() {
        return getWeiboRDD(DEFAULT_COLLECTION);
    }

    public JavaMongoRDD<Document> getUniversityRDD(String universityId) {
        return getWeiboRDD(universityId + "_weibo_info");
    }

    public Dataset<BaseInfo> getBaseInfoDataset() {
        return getBaseInfoRDD().toDS(BaseInfo.class);
    }

    public Dataset<WeiboItem> getUniversityDataset(String universityId) {
        JavaMongoRDD<Document> rdd = getUniversityRDD(universityId);
        return rdd.toDS(WeiboItem.class);
    }
}
