package streaming;

import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import entity.WeiboItem;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import util.MongoUtil;
import util.TimeUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SparkStreaming {
    private static List<String> universityIds = new ArrayList<>();

    public static void main(String[] args) {
        MongoUtil mongoUtil = MongoUtil.getMongoUtil();
        JavaMongoRDD<Document> baseInfoRDD = mongoUtil.getBaseInfoRDD();

        // Extract all ids of university
        baseInfoRDD.foreach(document -> SparkStreaming.universityIds.add(document.getString("id")));

        for (String universityId : universityIds) {
            JavaMongoRDD<Document> universityRDD = mongoUtil.getUniversityRDD("PKU");
            JavaRDD<Document> filteredRDD = universityRDD.filter(document -> {
                String time = document.getString("time");
                Date date = TimeUtil.parseDate(time);
                return date != null && TimeUtil.isBetween(date, 2018, 2019);
            });

            filteredRDD.foreach(document -> System.out.println(document.getString("time") + "\t" + document.getString("content")));
        }
    }
}
