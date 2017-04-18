package recommendationsystem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;


public class YelpRecommendationEngine {
	static int globcount =1;
	static JavaSparkContext sc;
	private static final String FILENAME = "/home/tushargupta98/Documents/BigData/output.txt";
	public static void main(String[] args) {


		// Turn off unnecessary logging
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Create Java spark context
		sc = new JavaSparkContext(SparkConfigurations.getSparkSession("host", "mongodb://localhost/yelp.sampleReviews2").sparkContext());

		JavaRDD<Rating> ratings = getRatingDetailsFromMongoDB(sc);

		JavaPairRDD<Integer, String> itemDescritpion = getBussDetailsFromMongoDB();
		JavaPairRDD<Integer, String> userDescritpion = getUserDetailsFromMongoDB();

		// Build the recommendation model using ALS

		int rank = 10; // 10 latent factors
		//	int numIterations = Integer.parseInt(args[2]); // number of iterations
		int numIterations = 10;	
		MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(ratings),
				rank, numIterations);

		// Create user-item tuples from ratings
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings
				.map(new Function<Rating, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(Rating r) {
						return new Tuple2<Object, Object>(r.user(), r.product());
					}
				});
		// Calculate the itemIds not rated by a particular user, say user with userId = 1
		while(globcount<2969){
			JavaRDD<Integer> notRatedByUser = userProducts.filter(new Function<Tuple2<Object,Object>, Boolean>() {

				public Boolean call(Tuple2<Object, Object> v1) throws Exception {
					if (((Integer) v1._1).intValue() != globcount) {
						return true;
					}
					return false;
				}
			}).map(new Function<Tuple2<Object,Object>, Integer>() {

				public Integer call(Tuple2<Object, Object> v1) throws Exception {
					return (Integer) v1._2;
				}
			});

			// Create user-item tuples for the items that are not rated by user, with user id 1
			JavaRDD<Tuple2<Object, Object>> itemsNotRatedByUser = notRatedByUser
					.map(new Function<Integer, Tuple2<Object, Object>>() {
						public Tuple2<Object, Object> call(Integer r) {
							return new Tuple2<Object, Object>(globcount, r);
						}
					});
			// Predict the ratings of the items not rated by user for the user
			JavaRDD<Rating> recomondations = model.predict(itemsNotRatedByUser.rdd()).toJavaRDD().distinct();
			// Sort the recommendations by rating in descending order
			recomondations = recomondations.sortBy(new Function<Rating,Double>(){

				public Double call(Rating v1) throws Exception {
					return v1.rating();
				}

			}, false, 1);	

			// Get top 10 recommendations
			JavaRDD<Rating> topRecomondations = sc.parallelize(recomondations.take(10));


			// Join top 10 recommendations with item descriptions
			JavaRDD<Tuple2<Rating, String>> recommendedItems = topRecomondations.mapToPair(
					new PairFunction<Rating, Integer, Rating>() {
						public Tuple2<Integer, Rating> call(Rating t) throws Exception {
							return new Tuple2<Integer,Rating>(t.product(),t);
						}
					}).join(itemDescritpion).values();
	
			JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
					recomondations.map(
							new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
								public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
									return new Tuple2(new Tuple2(r.user(), r.product()), r.rating());
								}
							}
							));
			
			JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
					JavaPairRDD.fromJavaRDD(ratings.map(
							new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
								public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
									return new Tuple2(new Tuple2(r.user(), r.product()), r.rating());
								}
							}
							)).join(predictions).values();
			double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
					new Function<Tuple2<Double, Double>, Object>() {
						public Object call(Tuple2<Double, Double> pair) {
							Double err = pair._1() - pair._2();
							return err * err;
						}
					}).rdd()).mean();
			//Print the top recommendations for user 1.
			writeToFile("Recommendations for User = "+globcount);
			if(recommendedItems.count()>0){
				recommendedItems.foreach(new VoidFunction<Tuple2<Rating,String>>() {
					public void call(Tuple2<Rating, String> t) throws Exception {
						writeToFile(t._1.product() + "\t" + t._1.rating() + "\t" + t._2);
					}
				});
			}
			writeToFile("Mean Squared Error = " + MSE);
			writeToFile("-----###-----");
			globcount++;
		}

	}

	private static JavaRDD<Rating> getRatingDetailsFromMongoDB(JavaSparkContext sc) {
		// Read user-item rating file. format - userId,itemId,rating
		JavaMongoRDD<Document> userItemRatingsFile = MongoSpark.load(sc);

		// Read item description file. format - itemId, itemName, Other Fields,..


		// Map file to Ratings(user,item,rating) tuples
		JavaRDD<Rating> ratings = userItemRatingsFile.map(new Function<Document, Rating>() {
			public Rating call(Document doc) {
				int userId = doc.getInteger("userIntId");
				int businessId = doc.getInteger("busninessIntId");
				double stars =doc.getDouble("stars");
				return new Rating(userId, businessId, stars);
			}
		});
		return ratings;
	}

	private static JavaPairRDD<Integer, String> getBussDetailsFromMongoDB() {
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "sampleBusinesses2");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sc).withOptions(readOverrides);

		// Load data using the custom ReadConfig
		JavaMongoRDD<Document> busRdd = MongoSpark.load(sc, readConfig);
		// Create tuples(itemId,ItemDescription), will be used later to get names of item from itemId
		JavaPairRDD<Integer,String> itemDescritpion = busRdd.mapToPair(
				new PairFunction<Document, Integer, String>() {

					public Tuple2<Integer, String> call(Document doc) throws Exception {
						int businessIntId  = doc.getInteger("businessIntId");
						String businessName = doc.getString("name");
						return new Tuple2<Integer,String>(businessIntId, businessName);
					}
				});
		return itemDescritpion;
	}
	private static JavaPairRDD<Integer, String> getUserDetailsFromMongoDB() {
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("collection", "sampleUsesrs2");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig readConfig = ReadConfig.create(sc).withOptions(readOverrides);

		// Load data using the custom ReadConfig
		JavaMongoRDD<Document> userRdd = MongoSpark.load(sc, readConfig);
		// Create tuples(itemId,ItemDescription), will be used later to get names of item from itemId
		JavaPairRDD<Integer,String> userDescritpion = userRdd.mapToPair(
				new PairFunction<Document, Integer, String>() {

					public Tuple2<Integer, String> call(Document doc) throws Exception {
						int userIntId  = doc.getInteger("userIntId");
						String userName = doc.getString("name");
						return new Tuple2<Integer,String>(userIntId, userName);
					}
				});
		return userDescritpion;
	}
	
	public static void writeToFile(String content) {

		BufferedWriter bw = null;
		FileWriter fw = null;

		try {

			fw = new FileWriter(FILENAME,true);
			bw = new BufferedWriter(fw);
			bw.write(content+"\n");

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}


	}
}
