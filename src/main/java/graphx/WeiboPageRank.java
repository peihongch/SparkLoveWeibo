package graphx;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.impl.GraphImpl;
import org.apache.spark.graphx.impl.VertexRDDImpl;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Function3;
import scala.Tuple2;
import scala.Tuple3;
import util.ResourceUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WeiboPageRank {
    public static final String REMOTE_MONGODB_URL = "mongodb://94.191.110.118:27017";
    public static final String LOCAL_MONGODB_URL = "mongodb://127.0.0.1:27017";
    public static final String DATABASE = "university_weibo";
    public static final String DEFAULT_COLLECTION = "follows_info";
    private static Properties universityNames;
    private static Map<Integer, String> universityHashTable;    // 因为GraphLoader要求节点必须是数字形式，因此需要将大学id转成其对于的hash码
    private static final String FOLLOWS_LIST = "follows_list_sampling.txt";
    private static final String UNIVERSITY_HASH_CODE = "university_hash_code.txt";
    private static SparkSession sparkSession;
    private static List<String> followsGraph = new LinkedList<>();

    private static void downloadFollowsGraph() throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(FOLLOWS_LIST)), true);
        PrintWriter uniHashCode = new PrintWriter(new OutputStreamWriter(new FileOutputStream(UNIVERSITY_HASH_CODE)), true);
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaMongoRDD<Document> followsInfoRDD = MongoSpark.load(jsc);
        followsInfoRDD.foreach(document -> {
            String targetId = (String) document.get("id");
            universityHashTable.put(targetId.hashCode(), targetId);
            List<Document> sourceIdList = document.getList("followsList", Document.class);
            sourceIdList.stream().filter(sourceId -> universityNames.containsKey(sourceId.get("id"))).forEach(
                    sourceId -> followsGraph.add(sourceId.get("id").hashCode() + " " + targetId.hashCode())
            );
        });
        followsGraph.forEach(pw::println);
        universityHashTable.forEach((key, value) -> uniHashCode.println(key + "\t" + value + "\t" + universityNames.getProperty(value)));
        pw.close();
        uniHashCode.close();
    }

    public static void main(String[] args) throws FileNotFoundException {
        universityNames = ResourceUtil.getUniversityList();
        universityHashTable = new Hashtable<>();
        sparkSession = SparkSession.builder().master("local[8]").appName("PageRank")
                .config("spark.mongodb.input.uri", REMOTE_MONGODB_URL + "/" + DATABASE + "." + DEFAULT_COLLECTION)
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.output.uri", LOCAL_MONGODB_URL + "/pagerank.result")
                .getOrCreate();

        try {
            downloadFollowsGraph();
        } catch (IOException e) {
            System.out.println("Failed to download follows graph!!!");
            System.exit(1);
        }

//        JavaRDD<String> javaRDD = new JavaSparkContext(sparkSession.sparkContext()).textFile("/home/steve/Documents/专业课程文档/云计算/作业/Java-PageRank/src/main/resources/university_list.txt", 2);
//        JavaPairRDD<String, String> pairRDD = javaRDD.mapToPair(rdd -> {
//            String[] urls = rdd.split("\\s+");
//            Tuple2<String, String> res = new Tuple2<>(urls[0], urls[1]);
//            return res;
//        });
//        pairRDD.foreach(stringStringTuple2 -> System.out.println(stringStringTuple2));
//        Graph graph = GraphLoader.edgeListFile(sparkSession.sparkContext(), FOLLOWS_LIST, false, -1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY());
//        Graph prGraph = PageRank.runUntilConvergence(graph, 0.001, 0.15, null, null);
//        VertexRDD joinedRDD = prGraph.vertices().leftJoin(pairRDD.rdd(), (v1,v2,v3)->{
//            Tuple3<String, String, String> res = new Tuple3<>((String) v1, (String) v2, (String) v3);
//            return res;
//        },null,null);
//        System.out.println(joinedRDD.first());
    }
}
