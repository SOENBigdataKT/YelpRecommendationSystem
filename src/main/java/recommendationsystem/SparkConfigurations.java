package recommendationsystem;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;

public class SparkConfigurations {

	
	public static SparkSession getSparkSession(String host, String collection) {
		MongoClient mongoClient = new MongoClient(host);
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.config("spark.mongodb.input.uri", collection)
				.getOrCreate();
		
		
		return spark;
	}
	public static Dataset<Row> createDataset(SparkSession session) {
		
		Dataset<Row> dsReview = MongoSpark.load(getJavaSparkContext(session)).toDF();

		return dsReview;
	}
	
	public static JavaSparkContext getJavaSparkContext(SparkSession session){
		
		JavaSparkContext jsc_Review = new JavaSparkContext(session.sparkContext());
		return jsc_Review;
	}
}
