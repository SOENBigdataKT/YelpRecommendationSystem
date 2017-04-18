package Business.RecomSystem;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class InsertToMongoDB {
	
	 @SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
	        MongoClient mongoClient = new MongoClient("localhost");
	       // List<String> databases = mongoClient.
	        
	        String JsonBusinessFile = "/home/karunsh/Desktop/yelp_academic_dataset_business.json";
	        String JsonReviewFile = "/home/karunsh/Desktop/yelp_academic_dataset_review.json";
	        String JsonUserFile = "/home/karunsh/Desktop/yelp_academic_dataset_user.json";
	        
	        MongoDatabase dataBase = mongoClient.getDatabase("yelpData");
	        MongoCollection<Document> collBusiness= dataBase.getCollection("YelpBusinessColl");
	        MongoCollection<Document> collReview= dataBase.getCollection("YelpReviewColl");
	        MongoCollection<Document> collUser = dataBase.getCollection("YelpUserColl");
	        //Collection Business
	        if (collBusiness.count() ==0)
		        {
	        	BufferedReader readerBusiness = new BufferedReader(new FileReader(JsonBusinessFile));
		        try {
		            String json;
	
		            while ((json = readerBusiness.readLine()) != null) {	                
		            	
		            	collBusiness.insertOne(Document.parse(json));
		               
		            } 
		        } finally {
		            readerBusiness.close();
		            System.out.println(collBusiness.count());
		        }
		        }
	        else
	        {
	        	System.out.println(collBusiness.count());
	        }
	        
	      //Collection Review
	        if (collReview.count() ==0)
	        {
        	BufferedReader readerReview = new BufferedReader(new FileReader(JsonReviewFile));
	        try {
	            String jsonReview;

	            while ((jsonReview = readerReview.readLine()) != null) {	                
	            	
	            	collReview.insertOne(Document.parse(jsonReview));
	               
	            } 
	        } finally {
	            readerReview.close();
	            System.out.println(collReview.count());
	        }
	        }
        else
        {
        	System.out.println(collReview.count());
        }
	        
	   // Collection user
	        if (collUser.count() ==0)
	        {
        	BufferedReader readerUser = new BufferedReader(new FileReader(JsonUserFile));
	        try {
	            String jsonUser;

	            while ((jsonUser = readerUser.readLine()) != null) {	                
	            	
	            	collUser.insertOne(Document.parse(jsonUser));
	               
	            } 
	        } finally {
	            readerUser.close();
	            System.out.println(collUser.count());
	        }
	        }
        else
        {
        	System.out.println(collUser.count());
        }   
	        
	        
	 }
	

}
