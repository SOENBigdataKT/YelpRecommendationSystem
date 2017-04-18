package yelp.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class YelpDataSetQueryExample {

	public static void main(String[] args){
	//	readViaDataFrames();
	//	getDatasetFromSQLContext();
		crossTwoDatasetsUsingRDD();
	}

	private static void readViaDataFrames(){
		SparkSession spark_Session = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.businesses");
		Dataset<Row> df = SparkConfigurations.createDataset(spark_Session);
		df.createOrReplaceTempView("businesses");
		Dataset<Row> sqlDF = spark_Session.sql("SELECT * FROM businesses");
		System.out.println(sqlDF.count());
	}
	
	private static void sqlContextExample(){
		SparkSession spark_Session = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.businesses");
		JavaSparkContext jsContext = SparkConfigurations.getJavaSparkContext(spark_Session);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(spark_Session.sparkContext());
	//	DataFrame table_01 = sqlContext.sql("SELECT * FROM customer");
	}
	
	private static void getDatasetFromSQLContext(){
		
		SparkSession spark_Session = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.businesses");
		Dataset<Row> df = SparkConfigurations.createDataset(spark_Session);
		df.createOrReplaceTempView("businesses");
		Dataset<Row> sqlDF = spark_Session.sql("SELECT * FROM businesses");
		SQLContext sqlcon = sqlDF.sqlContext();
		Dataset<Row> ds = sqlcon.sql("SELECT * FROM businesses WHERE state=\"QC\"");
		System.out.println(ds.count());
	}
	
	private static void joinTwoDataset(){
		SparkSession spark_Session = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.businesses");
		Dataset<Row> df = SparkConfigurations.createDataset(spark_Session);
		df.createOrReplaceTempView("businesses");
		SQLContext sqlConBusinesses = df.sqlContext();
		Dataset<Row> dsBusinesses = sqlConBusinesses.sql("SELECT business_id FROM businesses WHERE city=\"Montreal\"");
		System.out.println(dsBusinesses.count());
		
		SparkSession spark_Session_users = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.users");
		Dataset<Row> userdf = SparkConfigurations.createDataset(spark_Session_users);
		userdf.createOrReplaceTempView("users");
		SQLContext sqlConUsers = userdf.sqlContext();
		Dataset<Row> dsUsers = sqlConUsers.sql("SELECT user_id FROM users WHERE fans >200");
		System.out.println(dsUsers.count());
		System.out.println(dsBusinesses.count());
		
//		Dataset crossDataset = dsBusinesses.join(dsUsers);
//		System.out.println(crossDataset.count());
		}

	private static void crossTwoDatasetsUsingRDD(){
		SparkSession spark_Session = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.sampleBusinesses");
		Dataset<Row> df = SparkConfigurations.createDataset(spark_Session);
		df.createOrReplaceTempView("businesses");
		SQLContext sqlConBusinesses = df.sqlContext();
		Dataset<Row> dsBusinesses = sqlConBusinesses.sql("SELECT business_id FROM businesses");
		System.out.println(dsBusinesses.count());
	//	JavaMongoRDD<Document> businessRDD = MongoSpark.load(SparkConfigurations.getJavaSparkContext(spark_Session));
	//	System.out.println(businessRDD.cartesian(businessRDD).collect().size());
		JavaRDD businessRDD = dsBusinesses.javaRDD();
		
		
		SparkSession spark_Session_users = SparkConfigurations.getSparkSession("localhost", "mongodb://localhost/yelp.sampleUsers");
		Dataset<Row> userdf = SparkConfigurations.createDataset(spark_Session_users);
		userdf.createOrReplaceTempView("users");
		SQLContext sqlConUsers = userdf.sqlContext();
		Dataset<Row> dsUsers = sqlConUsers.sql("SELECT user_id FROM users ");
		System.out.println(dsUsers.count());	
		JavaRDD userRDD = dsUsers.javaRDD();
		System.out.println(businessRDD.cartesian(userRDD).collect().size());		
		spark_Session.stop();
		spark_Session_users.stop();
		}
}
