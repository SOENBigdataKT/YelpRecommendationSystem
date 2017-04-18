mongoexport -d yelp -c sampleUsers2 --jsonArray --out sampleUsers.json

mongoexport -d yelp -c sampleReviews2 --jsonArray --out sampleReviews.json

mongoexport -d yelp -c alsDataSet --jsonArray --out alsDataSet.json

mongoexport --host localhost --db yelp --collection sampleBusinesses2 --csv --out businesses.csv --fields businessIntId,name

mongoexport --host localhost --db yelp --collection sampleReviews2 --csv --out reviews.csv --fields userIntId,busninessIntId,stars

mongoexport --host localhost --db yelp --collection sampleReviews2 --csv --out reviews.csv --fields userIntId,busninessIntId,stars --sort "{userIntId:1}"