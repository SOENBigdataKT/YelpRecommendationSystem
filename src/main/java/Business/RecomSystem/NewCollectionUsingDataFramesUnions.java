package Business.RecomSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.bson.BSONObject;

import static org.apache.spark.sql.functions.col;

import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;

public final class NewCollectionUsingDataFramesUnions {
	static String uri = null;
	private SQLContext sqlContext;

	public static void main(final String[] args) throws InterruptedException {
	//	createSqlContext();
		queryUsingSpark();

	}

	private static void queryUsingSpark() {
		SparkSession spark_Session = null;
		ConfSetting conf = new ConfSetting();	
		
		
	/*	JavaSparkContext sc = getJavaSparkContext("localhost", "mongodb://localhost/yelp.businesses");
		SQLContext sqlContext = new SQLContext(sc);
		Dataset<Row> dsBusinesses = spark_Session.sql("Select * WHERE state=\"QC\"");
		dsBusinesses.show();
		System.out.println(dsBusinesses.count());
		spark_Session.stop();*/
		

		
/*		
  		String uri3 = statusconf(3);
  		spark_Session = conf.creatDataFrame(uri3);
		Dataset<Row> dsUser = creatDataset(spark_Session);
		dsUser.select("user_id", "name", "friends").show();
		spark_Session.stop();*/
		
		String uri1 = statusconf(1);

		// Create Dataset and connect with spark dataframe of yelp review
		spark_Session = conf.creatDataFrame(uri1);
	//	Dataset<Row> dsBusiness = creatDataset(spark_Session);
		Dataset<Row> df = spark_Session.read().load();
		df.createOrReplaceTempView("people");
		//dsBusiness.filter(col("state").equalTo("QC")).show();
		Dataset<Row> dsBusinesses = spark_Session.sql("Select * businesses WHERE state=\"QC\"");
		System.out.println(dsBusinesses.count());
		spark_Session.stop();

		/*	String uri2 = statusconf(2);
		// Create Dataset and connect with spark dataframe of yelp review
		spark_Session = conf.creatDataFrame(uri2);
		Dataset<Row> dsReview = creatDataset(spark_Session);
		dsReview.select("business_id", "user_id", "stars").show();
		System.out.println(dsReview.select("business_id", "user_id", "stars").count());
		spark_Session.stop();*/
	}

	public static String statusconf(int status1) {
		if (status1 == 1) {
			uri = "mongodb://localhost/yelp.businesses";
		} else if (status1 == 2) {
			uri = "mongodb://localhost/yelp.reviews";
		} else if (status1 == 3) {
			uri = "mongodb://localhost/yelp.users";
		}
		return uri;

	}

	public static Dataset<Row> creatDataset(SparkSession session) {
		JavaSparkContext jsc_Review = new JavaSparkContext(session.sparkContext());
		Dataset<Row> dsReview = MongoSpark.load(jsc_Review).toDF();

		return dsReview;
	}

	public static void sqlContextExample(){
		  JavaSparkContext sc = new JavaSparkContext(new SparkConf());
	        // Set configuration options for the MongoDB Hadoop Connector.
	        Configuration mongodbConfig = new Configuration();
	        // MongoInputFormat allows us to read from a live MongoDB instance.
	        // We could also use BSONFileInputFormat to read BSON snapshots.
	        mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

	        // MongoDB connection string naming a collection to use.
	        // If using BSON, use "mapred.input.dir" to configure the directory
	        // where BSON files are located instead.
	        mongodbConfig.set("mongo.input.uri",
	          "mongodb://localhost:27017/enron_mail.messages");

	        // Create an RDD backed by the MongoDB collection.
	        JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
	          mongodbConfig,            // Configuration
	          MongoInputFormat.class,   // InputFormat: read from a live cluster.
	          Object.class,             // Key class
	          BSONObject.class          // Value class
	        );

	        JavaRDD<Message> messages = documents.map(

	          new Function<Tuple2<Object, BSONObject>, Message>() {

	              public Message call(final Tuple2<Object, BSONObject> tuple) {
	                  Message m = new Message();
	                  BSONObject header =
	                    (BSONObject) tuple._2().get("headers");

	                  m.setTo((String) header.get("To"));
	                  m.setxFrom((String) header.get("From"));
	                  m.setMessageID((String) header.get("Message-ID"));
	                  m.setBody((String) tuple._2().get("body"));

	                  return m;
	              }
	          }
	        );

	        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

	        DataFrame messagesSchema = sqlContext.createDataFrame(messages, Message.class);
	        messagesSchema.registerTempTable("messages");

	        DataFrame ericsMessages = sqlContext.sql(
	          "SELECT to, body FROM messages WHERE to = \"eric.bass@enron.com\"");

	        ericsMessages.show();

	        messagesSchema.printSchema();
	}
	
	private static void createSqlContext(){
		SparkConf conf = new SparkConf().setAppName("Aggregation").setMaster("local");
		conf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		conf.set("mongo.input.uri", uri);
		conf.set("mongo.output.uri", uri);    
		conf.set("spark.mongodb.input.partitioner","MongoPaginateBySizePartitioner");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Map<String, String> options = new HashMap<String, String>();
		options.put("host","http://localhost:27017");
		options.put("uri", uri);
		options.put("database", "yelp");

		Dataset df = MongoSpark.read(sqlContext).options(options).option("collection", "businesses").load();
		
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