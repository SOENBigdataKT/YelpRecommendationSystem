package yelp.spark.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public final class ReadFromMongoDB {

  public static void main(final String[] args) throws InterruptedException {

    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost/yelp.sampleReviews2")
      .config("spark.mongodb.output.uri", "mongodb://localhost/yelp.sampleReviews2")
      .getOrCreate();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    /*Start Example: Read data from MongoDB************************/
    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    /*End Example**************************************************/

    // Analyze data from MongoDB
    System.out.println(rdd.count());
    System.out.println(rdd.first().toJson());

    jsc.close();

  }
}