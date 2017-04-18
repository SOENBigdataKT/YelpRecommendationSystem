package Business.RecomSystem;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.aggregate.First;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.SparkSession;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class ConfSetting {
	
	
	
		   
	        
	        public SparkSession creatDataFrame(String uri)
	        
	        {
	        	SparkSession spark_Session = null ;
	        	spark_Session = SparkSession.builder()
	        		      .master("local[*]")
	        		      .appName("YelpReviwe mongo db connected with spark")
	        		      .config("spark.mongodb.input.uri", uri)	        		      
	        		      .getOrCreate();
				return spark_Session;
	        }
		    
	
	
	
	

}