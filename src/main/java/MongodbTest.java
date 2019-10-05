import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongodbTest {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://94.191.110.118:27017");
        MongoDatabase database = mongoClient.getDatabase("university_weibo");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

//        for (String name:database.listCollectionNames()) {
//            var collection = database.getCollection(name);
//            collection.find().forEach(printBlock);
//        }
        database.getCollection("base_info").find().forEach(printBlock);

    }
}
