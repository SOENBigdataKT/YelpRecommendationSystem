package Business.RecomSystem;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class YelpALSDataset {

	DBCursor userDBcur;
	DBCursor businessDBcur;
	DBCollection users;
	DBCollection businesses;
	DBCollection reviews;
	DBCollection yelpAlsDataSet;
	DB db ;
	Mongo mongo;

	public static void main(String[] args) {

	//	printTime();
		YelpALSDataset obj = new YelpALSDataset();
		obj.createNewReviewDataset();
	//	printTime();
	}

	public YelpALSDataset(){
		mongo = new Mongo("localhost", 27017);
		db = mongo.getDB("yelp");

		users = db.getCollection("sampleUsers2");
		resetUserCursor();
		businesses = db.getCollection("sampleBusinesses2");
		resetBusinessCursor();
		reviews = db.getCollection("sampleReviews");
		yelpAlsDataSet = db.getCollection("alsDataSet");

	}
	private void resetUserCursor(){
		userDBcur = users.find();
	}

	public void createNewReviewDataset(){
		DBCollection reviews2 = db.getCollection("sampleReviews2");
		DBCollection reviews = db.getCollection("sampleReviews");
		DBCursor dbcur = reviews.find();
		int count=0;
		while(dbcur.hasNext()){
			DBObject o = dbcur.next();
			int userIntId=getUserIntId(o);
			int busninessIntId =getBusinessIntId(o);
			double stars =getReviewStars(o);
			BasicDBObject document = new BasicDBObject();
			document.put("userIntId",userIntId);
			document.put("busninessIntId",busninessIntId);
			document.put("stars",stars);
			reviews2.insert(document);
			System.out.println(count++);
		}
	}
	private int getBusinessIntId(DBObject o) {
		String businessId = (String) o.get("business_id");
		DBObject query = new BasicDBObject();
		query.put("business_id",businessId);
		DBObject curobj = businesses.findOne(query);
		return (Integer) curobj.get("businessIntId");
	}

	private double getReviewStars(DBObject o) {
		DBCollection reviews = db.getCollection("sampleReviews");
		String businessId = (String) o.get("business_id");
		String userId = (String) o.get("user_id");
		DBObject query = new BasicDBObject();
		List<BasicDBObject> andQuery = new ArrayList<BasicDBObject>();
		andQuery.add(new BasicDBObject("user_id", userId));
		andQuery.add(new BasicDBObject("business_id", businessId));
		query.put("$and", andQuery); 
		DBObject curobj = reviews.findOne(query);
		return Double.parseDouble(curobj.get("stars").toString());
	}

	private int getUserIntId(DBObject o) {
		// TODO Auto-generated method stub
		String userId = (String) o.get("user_id");
		DBObject query = new BasicDBObject();
		query.put("user_id",userId);
		DBObject curobj = users.findOne(query);
		return (Integer) curobj.get("userIntId");
		
	}

	private void resetBusinessCursor(){
		businessDBcur = businesses.find();
	}

	public void updateSampleUsers(){
		DBObject o;
		int count = 0;
		DBCollection users2 = db.getCollection("sampleUsers2");

		while(userDBcur.hasNext()){
			o = userDBcur.next();
			count++;
			o.put("userIntId", count);
			users2.insert(o);
		}
		
	}
	
	public void updateSampleBusiness(){
		DBObject o;
		int count = 0;
		DBCollection businesses2 = db.getCollection("sampleBusinesses2");

		while(businessDBcur.hasNext()){
			o = businessDBcur.next();
			count++;
			o.put("businessIntId", count);
			businesses2.insert(o);
		}
		
	}
	
	public void createALSDataset(){
		DBObject o;
		System.out.println("Program Start");
		printTime();
		String user_id="";
		String business_id="";
		float stars = 0;
		long count =0;
		while(userDBcur.hasNext()){
			o = userDBcur.next();
			user_id=(String) o.get("user_id") ;
			while(businessDBcur.hasNext()){
				o = businessDBcur.next();
				business_id=(String) o.get("business_id");
				stars = getRatingFromReview(user_id, business_id);
				updateAlsDataSet(user_id, business_id, stars);
			}
			resetBusinessCursor();
			count++;
			printTime();
			System.out.println(count);
		}
		System.out.println("Program End");

	}
	public static void printTime() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
	}
	public Float getRatingFromReview(String user_id, String business_id) {
		float stars = 0;
		DBObject query = new BasicDBObject();
		List<BasicDBObject> andQuery = new ArrayList<BasicDBObject>();
		andQuery.add(new BasicDBObject("user_id", user_id));
		andQuery.add(new BasicDBObject("business_id", business_id));
		query.put("$and", andQuery); 
		DBCursor cursor = reviews.find(query);
		if(cursor.hasNext()) {
			DBObject o=cursor.next();
			stars=Float.parseFloat((o.get("stars").toString()));
			reviews.remove(o);
		}
		return stars;
	}
	public void updateAlsDataSet(String user_id, String business_id, float stars) {
		BasicDBObject document = new BasicDBObject();
		document.put("user_id", user_id);
		document.put("business_id", business_id);
		document.put("stars", stars);
		yelpAlsDataSet.insert(document);
	}
	
	public void cleanBusinessesData(){
		
		DBObject query = new BasicDBObject();
		List<BasicDBObject> andQuery = new ArrayList<BasicDBObject>();
		andQuery.add(new BasicDBObject("is_open", 0));
	//	andQuery.add(new BasicDBObject("business_id", business_id));
		query.put("$and", andQuery); 
		DBCursor cursor = businesses.find(query);
		while(cursor.hasNext()) {
			DBObject o=cursor.next();
			businesses.remove(o);
		}
		
	}
}