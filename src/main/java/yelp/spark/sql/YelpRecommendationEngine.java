package yelp.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;
import scala.tools.nsc.Global;


public class YelpRecommendationEngine {
	static int globcount =0;
	public static void main(String[] args) {


		// Turn off unnecessary logging
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Create Java spark context
		JavaSparkContext sc = new JavaSparkContext(SparkConfigurations.getSparkSession("host", "mongodb://localhost/yelp.sampleReviews2").sparkContext());

		JavaRDD<Rating> ratings = getRatingDetailsFromMongoDB(sc);

		//	JavaPairRDD<Integer, String> itemDescritpion = getbusDetailsFromMongoDB();
		JavaPairRDD<Integer, String> itemDescritpion = getbusDetailsFromFile(sc);
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
		System.out.println(userProducts.count());
		System.out.println(userProducts.first());
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
							return new Tuple2<Object, Object>(0, r);
						}
					});
			//		System.out.println(itemsNotRatedByUser.count());
			// Predict the ratings of the items not rated by user for the user
			JavaRDD<Rating> recomondations = model.predict(itemsNotRatedByUser.rdd()).toJavaRDD().distinct();
			// Sort the recommendations by rating in descending order
			recomondations = recomondations.sortBy(new Function<Rating,Double>(){

				public Double call(Rating v1) throws Exception {
					return v1.rating();
				}

			}, false, 1);	
			//		System.out.println(recomondations.count());

			// Get top 10 recommendations
			JavaRDD<Rating> topRecomondations = sc.parallelize(recomondations.take(10));

			// Join top 10 recommendations with item descriptions
			JavaRDD<Tuple2<Rating, String>> recommendedItems = topRecomondations.mapToPair(
					new PairFunction<Rating, Integer, Rating>() {
						public Tuple2<Integer, Rating> call(Rating t) throws Exception {
							return new Tuple2<Integer,Rating>(t.product(),t);
						}
					}).join(itemDescritpion).values();


			//Print the top recommendations for user 1.
			
			if(recommendedItems.count()>0){
				System.out.println("User Id = "+globcount);
				System.out.println(recommendedItems.count());
				recommendedItems.foreach(new VoidFunction<Tuple2<Rating,String>>() {

					public void call(Tuple2<Rating, String> t) throws Exception {
						System.out.println(t._1.product() + "\t" + t._1.rating() + "\t" + t._2);

					}
				});
			}
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

		System.out.println(ratings.first());
		return ratings;
	}

	private static JavaPairRDD<Integer, String> getbusDetailsFromMongoDB() {
		JavaSparkContext businesssc = new JavaSparkContext(SparkConfigurations.getSparkSession("host", "mongodb://localhost/yelp.sampleBusinesses2").sparkContext());
		JavaMongoRDD<Document> itemDescritpionFile = MongoSpark.load(businesssc);
		// Create tuples(itemId,ItemDescription), will be used later to get names of item from itemId
		JavaPairRDD<Integer,String> itemDescritpion = itemDescritpionFile.mapToPair(
				new PairFunction<Document, Integer, String>() {

					public Tuple2<Integer, String> call(Document doc) throws Exception {
						int businessIntId  = doc.getInteger("businessIntId");
						String businessName = doc.getString("name");
						return new Tuple2<Integer,String>(businessIntId, businessName);
					}
				});
		System.out.println(itemDescritpion.first());
		return itemDescritpion;
	}

	private static JavaPairRDD<Integer, String> getbusDetailsFromFile(JavaSparkContext sc) {
		// Create tuples(itemId,ItemDescription), will be used later to get names of item from itemId
		// Read item description file. format - itemId, itemName, Other Fields,..


		JavaRDD<String> itemDescritpionFile = sc.textFile("business.data");
		JavaPairRDD<Integer,String> itemDescritpion = itemDescritpionFile.mapToPair(
				new PairFunction<String, Integer, String>() {

					public Tuple2<Integer, String> call(String t) throws Exception {
						String[] s = t.split(",");
						return new Tuple2<Integer,String>(Integer.parseInt(s[0]), s[1]);
					}
				});
		System.out.println(itemDescritpion.first());
		return itemDescritpion;
	}
}
