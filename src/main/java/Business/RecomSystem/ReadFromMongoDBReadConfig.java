package Business.RecomSystem;


import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.QueryBuilder;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


public final class ReadFromMongoDBReadConfig {

	public static void main(final String[] args) throws InterruptedException {
		JavaSparkContext jscUsers = getJavaSparkContext("localhost", "mongodb://localhost/yelp.users");
		JavaSparkContext jscBusinesses = getJavaSparkContext("localhost", "mongodb://localhost/yelp.businesses");
		/*Start Example: Read data from MongoDB************************/
		JavaMongoRDD<Document> rdd = MongoSpark.load(jscUsers);
		/*End Example**************************************************/

		// Analyze data from MongoDB
		//   System.out.println(rdd.count());
		System.out.println(rdd.first().toJson());//getString());
	/*	String dataSet = rdd.first().getString("user_id")
				+","+rdd.first().get("business_id").toString()
				+","+rdd.first().getInteger("stars");
		System.out.println(dataSet);//getString());
*/


		
		jsc.close();

	}

	public static JavaSparkContext getJavaSparkContext(String host, String collection) {
		MongoClient mongoClient = new MongoClient(host);
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", collection)
				.getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		return jsc;
	}
}
