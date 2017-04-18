package Business.RecomSystem;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;


public final class DatasetSQLDemo {

  public static void main(final String[] args) throws InterruptedException {

    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/yelp.reviews")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/yelp.reviews")
      .getOrCreate();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    // Load data and infer schema, disregard toDF() name as it returns Dataset
    Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
    implicitDS.printSchema();
    implicitDS.show();

    // Load data with explicit schema
    Dataset<Character> explicitDS = MongoSpark.load(jsc).toDS(Character.class);
  //  explicitDS.printSchema();
  //  explicitDS.show();

    // Create the temp view and execute the query
    explicitDS.createOrReplaceTempView("characters");
    Dataset<Row> centenarians = spark.sql("SELECT user_id, business_id, stars FROM characters");
    centenarians.show();

    // Write the data to the "hundredClub" collection
    MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

    // Load the data from the "hundredClub" collection
  //  MongoSpark.load(spark, ReadConfig.create(spark).withOption("collection", "hundredClub"), Character.class).show();

    jsc.close();

  }
}