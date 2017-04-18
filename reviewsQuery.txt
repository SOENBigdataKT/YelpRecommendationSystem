
db.reviews.aggregate([{       "$lookup": {            "from": "sampleBusinesses",            "localField": "business_id",            "foreignField": "business_id",            "as": "users"        }    },{$out: "sampleReviews"}])

db.sampleBusinesses.distnict("business_id").forEach(function(doc){  db.sampleReviews.insert(doc);});

db.reviews.find({business_id:{$in:db.sampleBusinesses.distinct("business_id")}}).forEach(function(doc){  db.sampleReviews.insert(doc);});

db.users.find({user_id:{$in:db.sampleReviews.distinct("user_id")}}).forEach(function(doc){  db.sampleUsers.insert(doc);});

db.users.find({user_id:{$in:db.sampleUsers.distinct("user_id")}}).forEach(function(doc){  db.sampleUsers.insert(doc);});

db.reviews.find({business_id:{$nin:["UTiG1oIeTKm6SQLqE8fD_A", "9SsjBLmevA18JmHngxtQJw", "v3RwgM8ILuDsx70SvBHAhQ", "OKZZpePG48tKExe4ct_2bg", "mA5nh8CZWdECUr1LEIZQWA"]}}).forEach(function(doc){  db.sampleReviews.insert(doc);});

db.createCollection("alsDataSet", { size: 2147483648 } )
534240

db.sampleBusinesses.distinct("business_id").count()

db.reviews.aggregate([ { $group: {"business_id":"$account" , "number":{$sum:1}} } ])

db.createCollection("sampleUsers2", { size: 2147483648 } )

db.createCollection("sampleBusinesses2", { size: 2147483648 } )
db.createCollection("sampleReviews2", { size: 2147483648 } )