db.yelpAlsDataSet.aggregate(
    [{ $unwind: '$users'},{ $unwind: '$businesses'}, 
        { "$group" : { "_id" : { "user_id": "$users.user_id","business_id": "$businesses.business_id", "stars":"0"}, }
        },
        { "$group" : { "_id" : null, 

            "result": { "$addToSet" : { "user_id" : "$_id.user_id", "name" : "$_id.business_id", "stars":"0" }
                    },   
        }
},
{"$project":{
     "_id":0,
     "result": 1,
 }
},]);