import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import entity.WeiboItem;
import org.apache.spark.sql.Dataset;
import org.bson.Document;

public class SparkMongo {
    public static void main(String[] args) {
        MongoUtil mongoUtil = MongoUtil.getMongoUtil();
        JavaMongoRDD<Document> pkuRdd = mongoUtil.getUniversityRDD("PKU");
        Dataset<WeiboItem> weiboItemDataset = pkuRdd.toDS(WeiboItem.class);
        weiboItemDataset.show();
    }
}
