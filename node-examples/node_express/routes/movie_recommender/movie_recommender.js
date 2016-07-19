var spark = require('eclairjs');
//var spark = require('/Users/jbarbetta/Work/gitProjects/eclairjs/eclairjs-node/lib/index.js');

var top25Recommendation;
var rebuildingTop25 = 'complete';
var dataCallback;
var new_ratings_model;
var complete_ratings_data
var complete_movies_titlesDF;
var best_rank = -1;
var iterations = 10;
var regularization_parameter = 0.1
var blocks = -1;
var seed = 0;
var complete_movies_data;
var complete_movies_titles;
var movie_rating_counts_RDD;
var userMovieRatingHash = {
		// movie id is key:
		"260": 9, // Star Wars (1977)
        "1": 8, // Toy Story (1995)
        "16": 7, // Casino (1995)
        "25": 8, // Leaving Las Vegas (1995)
        "32": 9, // Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
        "335": 4, // Flintstones, The (1994)
        "379": 3, // Timecop (1994)
        "296": 7, // Pulp Fiction (1994)
        "858": 10, // Godfather, The (1972)
        "50": 8 // Usual Suspects, The (1995)
};

var sparkConf = new spark.SparkConf(false)
	.set("spark.executor.memory", "10g")
	.set("spark.driver.memory", "6g")
	.setMaster(process.env.SPARK_MASTER || "local[*]")
	.setAppName("movie_recommender");
var sc = new spark.SparkContext(sparkConf);
var sqlContext = new spark.sql.SQLContext(sc);
var pathToSmallDataset = process.env.SMALL_DATASET;
var pathToCompleteDataset = process.env.LARGE_DATASET;

var start = new Date().getTime();

function failureExit(e) {
	res.send(e);
    sc.stop().then(function() {
	   // process.exit();
	  });
}

var small_ratings_raw_data = sc.textFile(pathToSmallDataset + '/ratings.csv');
small_ratings_raw_data.take(1).then(function(val) {
	  //console.log("Success:", val);
	  var small_ratings_raw_data_header = val[0];
	  var small_ratings_data = small_ratings_raw_data
	      .filter(function (line, small_ratings_raw_data_header) {
	          // filters out the header
	          return line != small_ratings_raw_data_header;
	      }, [small_ratings_raw_data_header])
	      .map(function (line, Rating) {
	          var tokens = line.split(",");
	          return new Rating(tokens[0], tokens[1], tokens[2]);
	      }, [spark.mllib.recommendation.Rating])
	      .cache();
	  
	  	var small_movies_raw_data = sc.textFile(pathToSmallDataset + '/movies.csv');
	    small_movies_raw_data.take(1).then(function(result){
	    	 var small_movies_raw_data_header = result[0];
	    	 var small_movies_data = small_movies_raw_data
		        .filter(function (line, small_movies_raw_data_header) {
		            // filters out the header
		            return line != small_movies_raw_data_header;
		        }, [small_movies_raw_data_header])
		        .map(function (line, Tuple2) {
		            var fields = line.split(",");
		            return new Tuple2(parseInt(fields[0]), fields[1]);
		        }, [spark.Tuple2]).cache();

		    var small_movies_titles = small_movies_data
		        .mapToPair(function (tuple2, Tuple2) {
		            return new Tuple2(tuple2._1(), tuple2._2());
		        }, [spark.Tuple2]);
		    
		    small_movies_titles.take(3).then(function(result){
		    	  console.log("small_movies_titles :", result);
		    	  
		    	  	seed = 0;
		    	    small_ratings_data.randomSplit([0.6, 0.2, 0.2], seed).then(function(split){
		    	    	var training_RDD = split[0];
			    	    var validation_RDD = split[1];
			    	    //var test_RDD = split[2];

			    	    var validation_for_predict_RDD = validation_RDD.map(function (rating, Tuple2) {
			    	        return new Tuple2(rating.user(), rating.product());

			    	    }, [spark.Tuple2]);

			    	    seed = 5;
			    	    //var iterations = 10;
			    	    //var regularization_parameter = 0.1
			    	    var ranks = [4, 8, 12];
			    	    var errors = [0, 0, 0];
			    	    var err = 0;

			    	    var min_error = Number.POSITIVE_INFINITY;
			    	    //var best_rank = -1;
			    	    //var blocks = -1;
			    	    var promises = [];
			    	    ranks.forEach(function (rank) {
			    	        var model = spark.mllib.recommendation.ALS.train(training_RDD, rank, iterations, regularization_parameter, blocks, seed);
			    	        var predictions = model.predict(validation_for_predict_RDD)
			    	            .mapToPair(function (rating, Tuple2) {
			    	                    return new Tuple2(new Tuple2(rating.user(), rating.product()), rating.rating());
			    	                }, [spark.Tuple2]
			    	            );

			    	        var rates_and_preds = validation_RDD
			    	            .mapToPair(function (rating, Tuple2) {
			    	                return new Tuple2(new Tuple2(rating.user(), rating.product()), rating.rating());
			    	            }, [spark.Tuple2])
			    	            .join(predictions);

			    	        var t = rates_and_preds
			    	            .mapToFloat(function (tuple) {
			    	                return Math.pow(tuple._2()._1() - tuple._2()._2(), 2);
			    	            });
			    	        promises.push(t.mean());
			    	    });
			    	    Promise.all(promises).then(function(values) {
			    	    	//console.log("Promise.all " + values);
			    	    	for (var i = 0; i < values.length; i++){
			    	    		var error = Math.sqrt(values[i]);
			    	        	errors[err] = error;
				    	        err += 1;
				    	        if (error < min_error) {
				    	            min_error = error;
				    	            best_rank = ranks[i];
				    	        }
			    	    	}
			    	    	console.log("The best model was trained with rank " + best_rank);
			    	    	
			    	    	 /*
			    	        In order to build our recommender model, we will use the complete dataset.

			    	        */
			    	       var complete_ratings_raw_data = sc.textFile(pathToCompleteDataset + '/ratings.csv');
			    	       complete_ratings_raw_data.take(1).then(function(result){
			    	    	   var complete_ratings_raw_data_header = result[0];
				    	       complete_ratings_data = complete_ratings_raw_data
				    	           .filter(function (line, complete_ratings_raw_data_header) {
				    	               return line != complete_ratings_raw_data_header;
				    	           }, [complete_ratings_raw_data_header])
				    	           .map(function (line, Rating) {
				    	               var fields = line.split(",");
				    	               var userId = parseInt(fields[0]);
				    	               var movieId = parseInt(fields[1]);
				    	               var rating = parseFloat(fields[2]);
				    	               return new Rating(userId, movieId, rating);
				    	           }, [spark.mllib.recommendation.Rating])
				    	           .cache();

				    	       complete_ratings_data.randomSplit([0.7, 0.3], 0).then(function(splits2){
				    	    	   var training_RDD = splits2[0];
					    	       var test_RDD = splits2[1];
		
					    	       var complete_model = spark.mllib.recommendation.ALS.train(training_RDD, best_rank, iterations, regularization_parameter, blocks, seed);
					    	       /*
					    	       Now we test on our testing set.
					    	       */
					    	      var test_for_predict_RDD = test_RDD
					    	          .map(function (rating, Tuple2) {
					    	              return new Tuple2(rating.user(), rating.product());
					    	          }, [spark.Tuple2]);

					    	      var predictions = complete_model.predict(test_for_predict_RDD)
					    	          .mapToPair(function (rating, Tuple2) {
					    	              return new Tuple2(new Tuple2(rating.user(), rating.product()), rating.rating());
					    	          }, [spark.Tuple2]);

					    	      var rates_and_preds = test_RDD
					    	          .mapToPair(function (rating, Tuple2) {
					    	              return new Tuple2(new Tuple2(rating.user(), rating.product()), rating.rating());
					    	          }, [spark.Tuple2])
					    	          .join(predictions);


					    	      var t = rates_and_preds
					    	          .mapToFloat(function (x) {
					    	              return Math.pow(x._2()._1() - x._2()._2(), 2);
					    	          });
					    	      t.mean().then(function(mean){
					    	    	  var error = Math.sqrt(mean);
						    	      console.log("For testing data the RMSE is " + error);
						    	      
						    	      /*
						    	      How to make recommendations
						    	      So let's first load the movies complete file for later use.
						    	      */

						    	     var complete_movies_raw_data = sc.textFile(pathToCompleteDataset + '/movies.csv');
						    	     complete_movies_raw_data.take(1).then(function(result){
						    	    	 var complete_movies_raw_data_header = result[0];
						    	    	 complete_movies_data = complete_movies_raw_data
						    	         .filter(function (line, complete_movies_raw_data_header) {
						    	             // filters out the header
						    	             return line != complete_movies_raw_data_header;
						    	         }, [complete_movies_raw_data_header])
						    	         .map(function (line, Tuple2) {
						    	             var fields = line.split(",");
						    	             var x = parseInt(fields[0]);
						    	             return new Tuple2(x, fields[1]);
						    	         }, [spark.Tuple2]).cache();

							    	     complete_movies_titles = complete_movies_data
							    	         .mapToPair(function (tuple2, Tuple2) {
							    	             return new Tuple2(tuple2._1(), tuple2._2());
							    	         }, [spark.Tuple2]);
							    	     
							    	     
							    	   //Generate the schema
							    	     var DataTypes = spark.sql.types.DataTypes;

							    	     var fields = [];
							    	     fields.push(DataTypes.createStructField("id", DataTypes.IntegerType, true));
							    	     fields.push(DataTypes.createStructField("title", DataTypes.StringType, true));
							    	     
							    	     var schema = DataTypes.createStructType(fields);
							    	     var rowRDD = complete_movies_data.map(function (tuple2, RowFactory) {
						    	             return RowFactory.create([tuple2._1(), tuple2._2()]);
						    	         }, [spark.sql.RowFactory]);

							    	     //Apply the schema to the RDD.
							    	     complete_movies_titlesDF = sqlContext.createDataFrame(rowRDD, schema);
							    	     
							    	     /*
							    	     Another thing we want to do, is give recommendations
							    	     of movies with a certain minimum number of ratings. For that, we need to count the number of ratings per movie.
							    	     */
							    	    var movie_ID_with_ratings_RDD = complete_ratings_data
							    	        .mapToPair(function (rating, Tuple2) {
							    	            return new Tuple2(rating.product(), rating.rating());
							    	        }, [spark.Tuple2])
							    	        .groupByKey();
	
							    	    var movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD
							    	        .mapToPair(function (ID_and_ratings_tuple, Tuple2) {
							    	            var w = ID_and_ratings_tuple._2();
							    	            var count = 0;
							    	            var sum = 0;
							    	            for (var i = 0; i < w.length; i++) {
							    	                var r = w[i];
							    	                sum += r;
							    	                count++;
							    	            }
							    	            var avgRating = sum / count;
							    	            return new Tuple2(ID_and_ratings_tuple._1(), new Tuple2(count, avgRating));
							    	        }, [spark.Tuple2]);
	
							    	    movie_rating_counts_RDD = movie_ID_with_avg_ratings_RDD
							    	        .mapToPair(function (ID_with_avg_ratings, Tuple2) {
							    	            var coutAvg = ID_with_avg_ratings._2();
							    	            return new Tuple2(ID_with_avg_ratings._1(), coutAvg._1()); // movieID, rating count
							    	        }, [spark.Tuple2]);
	
							    	    /*
							    	     Now we need to rate some movies for the new user.
							    	     */
							    	    rateMoviesForUser();

						    	     }, failureExit);
					    	      }, failureExit); 
				    	       }, failureExit);
			    	       }, failureExit);
			    	    }, failureExit);
		    	    }, failureExit);	  
	    }, failureExit);
      }, failureExit);
	}, function(err) {
		res.send("Error:", err);
	  sc.stop().then(function() {
	   // process.exit();
	  });
	});
/**
 * 
 * Updates the top25 movies for the user, this runs "in the background" after the user updates his ratings
 */
function rateMoviesForUser(cb) {
		rebuildTop25 = 'inProgress';

		/*
	    Now we need to rate some movies for the new user.
	    */

	   var new_user_ID = 0;
	   var new_user_ratings = [];
	   //  get just movie IDs
	   var new_user_ratings_ids = [];
	   for (var key in userMovieRatingHash) {
		// The format of each line is (userID, movieID, rating)
	   	new_user_ratings.push(new spark.mllib.recommendation.Rating(new_user_ID, key, userMovieRatingHash[key]));
	   	new_user_ratings_ids.push(key); //  get just movie IDs
	   }
	   

	   var new_user_ratings_RDD = sc.parallelize(new_user_ratings);
	   /*
	    Now we add them to the data we will use to train our recommender model.
	    */
	   var complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD);

	   //var new_ratings_model = spark.mllib.recommendation.ALS.train(complete_data_with_new_ratings_RDD, best_rank, iterations, regularization_parameter, blocks, seed);
	   new_ratings_model = spark.mllib.recommendation.ALS.train(complete_data_with_new_ratings_RDD, best_rank, iterations, regularization_parameter, blocks, seed);

	   /*
	    Let's now get some recommendations
	    */

	   	 // keep just those not on the ID list
	    var new_user_unrated_movies_RDD = complete_movies_data
	        .filter(function (tuple, userMovieRatingHash) {
	            if (userMovieRatingHash[tuple._1()]) {
	            	// User has already rated this move to remove from list
	                return false;
	            } else {
	            	// keep this movie
	                return true;
	            }
	        }, [userMovieRatingHash])
	        .map(function (tuple, new_user_ID, Tuple2) {
	            return new Tuple2(new_user_ID, tuple._1());
	        }, [new_user_ID, spark.Tuple2]);

	    var new_user_recommendations_RDD = new_ratings_model.predict(new_user_unrated_movies_RDD);

	    // Transform new_user_recommendations_RDD into pairs of the form (Movie ID, Predicted Rating)
	    var new_user_recommendations_rating_RDD = new_user_recommendations_RDD
	        .mapToPair(function (rating, Tuple2) {
	            return new Tuple2(rating.product(), rating.rating());
	        }, [spark.Tuple2]);

	    var new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_RDD
	        .join(complete_movies_titles)
	        .join(movie_rating_counts_RDD);
	    /*
	     So we need to flat this down a bit in order to have (Title, Rating, Ratings Count).
	     */

	    var new_user_recommendations_rating_title_and_count_RDD2 = new_user_recommendations_rating_title_and_count_RDD
	        .map(function (t, Tuple4) {
	            var x = new Tuple4(t._2()._1()._2(), t._2()._1()._1(), t._2()._2(), t._1());
	            return x;
	        }, [spark.Tuple4]);
	    new_user_recommendations_rating_title_and_count_RDD2.take(3).then(function(result){
	    	console.log("new_user_recommendations_rating_title_and_count_RDD2" + JSON.stringify(result));
	    	
	    	/*
	        Finally, get the highest rated recommendations for the new user, filtering out movies with less than 25 ratings.
	        */


	       var new_user_recommendations_rating_title_and_count_RDD2_filtered = new_user_recommendations_rating_title_and_count_RDD2
	           .filter(function (tuple4) {
	               if (tuple4._3() < 25) {
	                   return false;
	               } else {
	                   return true;
	               }
	           });

	       /*
	        list top 25
	        */

	       new_user_recommendations_rating_title_and_count_RDD2_filtered
	           .takeOrdered(25, function (tuple4_a, tuple4_b) {
	               var aRate = tuple4_a._2();
	               var bRate = tuple4_b._2();
	               return aRate > bRate ? -1 : aRate == bRate ? 0 : 1;

          }).then(function(top_movies){
       	   console.log("TOP recommended movies (with more than 25 reviews):");
   	       for (var i = 0; i < top_movies.length; i++) {
   	           console.log(JSON.stringify(top_movies[i]));
   	       }
   	       top25Recommendation = top_movies;
   	       console.log("top 25 updated");
   	       //resolve(top_movies);
           if (cb) {
        	   cb(top_movies)
    	   };
          }, failureExit);
	    });
	    

};

function predictRating(movies, callback) {
	 /*
    Another useful usecase is getting the predicted rating for a particular movie for a given user.
    */
	var m = [];
	movies.forEach(function(movie){
		m.push(new spark.Tuple2(0, movie._values[0]))
	});
   var my_movie = sc.parallelizePairs(m); // Quiz Show (1994)
   var individual_movie_rating_RDD = new_ratings_model.predict(my_movie);
   individual_movie_rating_RDD.take(10).then(function(result){
 	  console.log("Predicted rating for movie " + JSON.stringify(result));
 	  callback(result);

   },failureExit);
}

/**
 * REST Service returns JSON
 */
exports.top25 = function(req, res){
	if (top25Recommendation) {
		res.send("Top 25 " + JSON.stringify(top25Recommendation));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};

/**
 * REST Service returns JSON
 */
exports.predictedRatingForMovie = function(req, res){
	if (top25Recommendation) {
	// Register the DataFrame as a table.
		var id = parseInt(req.query.id); // FIXME get returns a promise, why? the data is right in the object
		console.log("id " + id);
		 var my_movie = sc.parallelizePairs([new spark.Tuple2(0, id)]); // 500 = Quiz Show (1994)
	     var individual_movie_rating_RDD = new_ratings_model.predict(my_movie);
	     individual_movie_rating_RDD.take(1).then(function(result){
		 res.send( JSON.stringify(result));
	     }, failureExit);
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}

};

/**
 * REST Service returns JSON
 * Returns movies that have the words in their title
 */
exports.movieID = function(req, res){
	if (top25Recommendation) {
		// Register the DataFrame as a table.
		//var col = complete_movies_titlesDF.col("id");
		var col2 = complete_movies_titlesDF.col("title");
	    var testCol = col2.contains(req.query.movie /*"Father of the Bride"*/);
	    var result = complete_movies_titlesDF.filter(testCol);
		result.take(10).then(function(r){
			predictRating(r, function(results){
				var movies = []
				r.forEach(function(movie){
					var m = {}
					m.id = movie._values[0];
					m.title = movie._values[1];
					results.forEach(function(pr){
						if (pr.product == m.id) {
							m.rating = pr.rating;
						}
					});
					movies.push(m);
				});
				res.send( JSON.stringify(movies));
	            // If this is result for a movieSearch send whatever current
	            // top25 is to websocket.
	            if (req.query.movieSearch === 'true' && top25Recommendation) {
	                //console.log("User movieSearch - send top25 to web socket");
	                handleTop25Update(top25Recommendation, true);
	            }
			});
			
	     }, failureExit);
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
	
};

/**
 * REST Service returns JSON
 * Returns for for the given id
 */
exports.movieTitle = function(req, res){
	if (top25Recommendation) {
		// Register the DataFrame as a table.
		 var col = complete_movies_titlesDF.col("id");
		 //var col2 = complete_movies_titlesDF.col("title");
	    var testCol = col.equalTo(req.query.id);
	    var result = complete_movies_titlesDF.filter(testCol);
		result.take(10).then(function(r){
			res.send( JSON.stringify(r));
	     }, failureExit);
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};

/**
 * REST Service returns JSON
 * Updates the ratings movie ratings for this user and then re-run the movie recommender predictions
 */
exports.rateMovie = function(req, res){
	if (top25Recommendation) {
		//var rating = { "id":req.body.id, "rating":req.body.rating};
		userMovieRatingHash[req.query.id] = parseInt(req.query.rating);
	    // update the predicted ratings for this user
	    // top25Update will be broadcast over websocket
		if (rebuildingTop25 == 'complete') {
			rebuildingTop25 = 'inprogress';
			rateMoviesForUser(handleTop25Update);
		} else {
			rebuildingTop25 = 'needed';
		}
		
	    // send back userHash
	    res.send(JSON.stringify(userMovieRatingHash));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};

function handleTop25Update(updatedTop25, fromSearch){
    //console.log("handleTop25Update: ",JSON.stringify(updatedTop25));
    var top25 = [];
    updatedTop25.forEach(function(movie) {
        m = {};
        m.id = movie[3];
        m.title = movie[0];
        m.rating = movie[1];
        m.numberOfRatings = movie[2];
        top25.push(m);
    });
    if (dataCallback) {
    	dataCallback(JSON.stringify({type:'top25Update', data: top25, original25: fromSearch || false}));
	}
    if (rebuildingTop25 == 'needed') {
    	rebuildingTop25 = 'inprogress';
    	rateMoviesForUser(handleTop25Update);
    } else {
    	rebuildingTop25 = 'complete';
    }

}

/**
 * Start sending data updates to node server so it can farm out to clients attatched to websocket
 */
exports.startUpdates = function(cb) {
    dataCallback = cb;
};

exports.rate = function(req, res){
  res.render('movie_recommender/rate', { title: 'Movie Recommender' });
};
