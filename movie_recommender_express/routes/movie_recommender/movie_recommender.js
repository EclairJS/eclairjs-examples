var eclairjs = require('eclairjs');
var timers = require('timers');

var spark; 
var spark2; 

/*
 * Globals
 */

var pathToSmallDataset = process.env.SMALL_DATASET;
var pathToCompleteDataset = process.env.LARGE_DATASET;

var use_saved_data = process.env.USE_SAVED_DATA || false;
var saved_data_path = process.env.SAVED_DATA || "/tmp";
var complete_ratings_data_path = saved_data_path +'/complete_ratings_data';
var predictionModleValuesDFPath = saved_data_path + '/predictionModleValuesDF';
var complete_movies_titlesDFPath = saved_data_path + '/complete_movies_titlesDF';
var complete_movies_data_path = saved_data_path + '/complete_movies_data';
var complete_movies_titles_path = saved_data_path + '/complete_movies_titles';
var movie_rating_counts_RDD_path = saved_data_path + '/movie_rating_counts_RDD';
var new_user_recommendations_rating_title_and_count_DF_filtered_path = saved_data_path + '/new_user_recommendations_rating_title_and_count_DF_filtered';
var new_user_recommendations_rating_title_and_count_DF_path = saved_data_path + '/new_user_recommendations_rating_title_and_count_DF';
var new_ratings_model_path = saved_data_path + '/new_ratings_model';
var warehouseLocation = saved_data_path +"/spark-warehouse";

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
var new_user_recommendations_rating_title_and_count_DF_filtered;
var predictionModleValuesDF;
var new_user_recommendations_rating_title_and_count_DF;


/*
 * a starter group of movies so we have something for the first pridtions
 */
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

/**
 * Handle errors
 * @param e
 */
function failureExit(e) {
	res.send(e);
    sc.stop().then(function() {
	   // process.exit();
	  });
}

/**
 * Builds the RDDs and model needed to recommend movies, this can take a while as the 
 * data set is quite large. We do save the computed RDDs and models so on futures server start ups
 * they can be reused.
 */
function movie_recommender_init() {
	var sparkSession = getSparkSessionForModel();
	var sc = sparkSession.sparkContext();
	configForSwift(sc);
    console.log("starting movie_recommender_init.....");
	/*
	 * Load the small movielens data sets we will use them to train the model.
	 */
	var small_ratings_raw_data = sc.textFile(pathToSmallDataset + '/ratings.csv');
	var x = small_ratings_raw_data.randomSplit([0.6, 0.2, 0.2], 0).then(function(split){
	    	console.log("#### " + split.length);
	  });
	small_ratings_raw_data.take(1).then(function(val) {
		  console.log("Success:", val);
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
		  small_ratings_data.randomSplit([0.6, 0.2, 0.2], 0).then(function(split){
  	    	var training_RDD = split[0];
	    	    var validation_RDD = split[1];
		  });
		  // end hacking
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
			    	  	/*
			    	  	 * split the data set for training
			    	  	 */
			    	    small_ratings_data.randomSplit([0.6, 0.2, 0.2], seed).then(function(split){
			    	    	var training_RDD = split[0];
				    	    var validation_RDD = split[1];
				    	    var validation_for_predict_RDD = validation_RDD.map(function (rating, Tuple2) {
				    	        return new Tuple2(rating.user(), rating.product());
				    	    }, [spark.Tuple2]);
				    	    seed = 5;
				    	    var ranks = [4, 8, 12];
				    	    var errors = [0, 0, 0];
				    	    var err = 0;
				    	    var min_error = Number.POSITIVE_INFINITY;
				    	    var promises = [];
				    	    /*
				    	     * determine the best rank to use.
				    	     */
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
				    	    	console.log("Promise.all " + values);
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
				    	         * In order to build our recommender model, we will use the complete dataset.
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
 
								    	   // Generate the schema movie titles Dataframe
								    	     var DataTypes = spark.sql.types.DataTypes;

								    	     var fields = [];
								    	     fields.push(DataTypes.createStructField("id", DataTypes.IntegerType, true));
								    	     fields.push(DataTypes.createStructField("title", DataTypes.StringType, true));
								    	     
								    	     var schema = DataTypes.createStructType(fields);
								    	     var rowRDD = complete_movies_data.map(function (tuple2, RowFactory) {
							    	             return RowFactory.create([tuple2._1(), tuple2._2()]);
							    	         }, [spark.sql.RowFactory]);

								    	     //Apply the schema to the RDD and create Dataframe
								    	     complete_movies_titlesDF = sparkSession.createDataFrame(rowRDD, schema);
								    	     
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

                                            if (use_saved_data) {		
                                                /*
                                                 * Save what we have built for quick server spin up on future starts
                                                 */
                                                complete_ratings_data.saveAsObjectFile(complete_ratings_data_path, true);
                                                complete_movies_data.saveAsObjectFile(complete_movies_data_path, true);
                                                complete_movies_titles.saveAsObjectFile(complete_movies_titles_path, true);
                                                movie_rating_counts_RDD.saveAsObjectFile(movie_rating_counts_RDD_path, true);

                                                /*
                                                 * save the valuse used by the ALS prediction model in a Dataframe
                                                 */
                                                var DataTypes = spark.sql.types.DataTypes;
									
                                                var fields = [];
                                                fields.push(DataTypes.createStructField("best_rank", DataTypes.IntegerType, true));
                                                fields.push(DataTypes.createStructField("iterations", DataTypes.IntegerType, true));
                                                fields.push(DataTypes.createStructField("regularization_parameter", DataTypes.FloatType, true));
                                                fields.push(DataTypes.createStructField("blocks", DataTypes.IntegerType, true));
                                                fields.push(DataTypes.createStructField("seed", DataTypes.IntegerType, true));
                                                 
                                                var schema = DataTypes.createStructType(fields);

                                                var row = spark.sql.RowFactory.create([best_rank, iterations, regularization_parameter, blocks, seed]);
                                                predictionModleValuesDF = sparkSession.createDataFrame([row], schema);
                                                predictionModleValuesDF.take(1).then(function(result){
                                                    console.log("predictionModleValuesDF result: ",result);
                                                    predictionModleValuesDF.write().mode('overwrite').parquet(predictionModleValuesDFPath);
                                                }, function(err){
                                                    console.log('err ' + err);
                                                });	
                                                 
                                                complete_movies_titlesDF.write().mode('overwrite').parquet(complete_movies_titlesDFPath);
                                            }
								    	   
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
}



/**
 * Updates the top25 movies for the user, this runs "in the background" after the user updates his ratings
 * @param {function} cb callback
 */
function rateMoviesForUser(cb) {
	var sparkSession = getSparkSessionForModel();
	var sc = sparkSession.sparkContext();
        console.log("rating movies for user");
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

	   new_ratings_model = spark.mllib.recommendation.ALS.train(complete_data_with_new_ratings_RDD, best_rank, iterations, regularization_parameter, blocks, seed);
	   new_ratings_model.save(sc, new_ratings_model_path, true);


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

	    var new_user_recommendations_rating_title_and_count_RDD_join1 = new_user_recommendations_rating_RDD
	        .join(complete_movies_titles);
    	var new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_title_and_count_RDD_join1
    		.join(movie_rating_counts_RDD);
	    
	    /*
	     So we need to flat this down a bit in order to have (Title, Rating, Ratings Count).
	     */
	    
	    // Generate the schema for top 25 movies Dataframe
  	     var DataTypes = spark.sql.types.DataTypes;

  	     var fields = [];
  	     fields.push(DataTypes.createStructField("title", DataTypes.StringType, true));
  	     fields.push(DataTypes.createStructField("predicted_ratings", DataTypes.FloatType, true));
  	     fields.push(DataTypes.createStructField("ratings_count", DataTypes.IntegerType, true));
  	     fields.push(DataTypes.createStructField("id", DataTypes.IntegerType, true));
  	     
  	     var top25MovieSchema = DataTypes.createStructType(fields);

  	     
	    var new_user_recommendations_rating_title_and_count_RDD2 = new_user_recommendations_rating_title_and_count_RDD
	        .map(function (t, RowFactory) {
	        	// Tuple4 format = (Title, Predicted Rating, Ratings Count, Movie ID
	            return RowFactory.create([t._2()._1()._2(), t._2()._1()._1(), t._2()._2(), t._1()]);

	        }, [ spark.sql.RowFactory]);
	     
	     new_user_recommendations_rating_title_and_count_RDD2.take(3).then(function(result){
	    	console.log("new_user_recommendations_rating_title_and_count_RDD2" + JSON.stringify(result));
	    	
   	     //Apply the schema to the RDD and create Dataframe
	       var local_new_user_recommendations_rating_title_and_count_DF = sparkSession.createDataFrame(new_user_recommendations_rating_title_and_count_RDD2
    		   																				, top25MovieSchema);
	       local_new_user_recommendations_rating_title_and_count_DF.write()
	       		.mode('overwrite')	
	       		.parquet(new_user_recommendations_rating_title_and_count_DF_path).then(function(){

				       refreshMovieRatingsDF(function(){
				    	   console.log("done updating DF after writting");
				       }); // now we need to update the DF used for reading the predictions.
				});
	      
   	     
	    	/*
	        Finally, get the highest rated recommendations for the new user, filtering out movies with less than 25 ratings.
	        */
	       
	       new_user_recommendations_rating_title_and_count_DF_filtered = local_new_user_recommendations_rating_title_and_count_DF
	       																	.filter("ratings_count > 24");
	       new_user_recommendations_rating_title_and_count_DF_filtered.write()
	       		.mode('overwrite')
	       		.parquet(new_user_recommendations_rating_title_and_count_DF_filtered_path);	   	    
	      
	       getTop25FromDF(new_user_recommendations_rating_title_and_count_DF_filtered, function(top_movies) {
	    	   top25Recommendation = top_movies;

	    	   if(cb) {
	    		   cb(top_movies);
	    	   }
	    	   if (rebuildingTop25 == 'needed') {
		    	   console.log("Pending rating, run model again");
			    	rebuildingTop25 = 'inprogress';
			    	rateMoviesForUser(handleTop25Update);
			    } else {
			    	rebuildingTop25 = 'complete';
			    	sessionStopTimer = timers.setTimeout(stopSparkSessionForModel(), 5000);
			    }
	    	   
			   
    	   });
     });
	    

};
var sessionStopTimer;
/**
 * Returns the top 25 rated movies from the RDD by passing them as a argument to the callback function
 * @param rdd
 * @param {function<array>} callback
 */
function getTop25FromDF(df, callback){
	 /*
    list top 25
    */
	console.log("TOP recommended movies :");
	df.sort(df.col("predicted_ratings").desc()).take(25)
  .then(function(top_movies){
   	   console.log("TOP recommended movies (with more than 25 reviews):");
	       for (var i = 0; i < top_movies.length; i++) {
	           console.log(JSON.stringify(top_movies[i]));
	       }
	       console.log("top 25 updated");
       if (callback) {
    	   callback(top_movies)
	   };
      }, 
  failureExit);
}


/**
 * REST Service returns JSON, send top 25 movies as JSON
 * @parma {httpRequest} req
 * @param {httpResponse} res
 */
exports.top25 = function(req, res){
	if (top25Recommendation) {
		res.send("Top 25 " + JSON.stringify(top25Recommendation));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};

/**
 * REST Service returns JSON, pridect the users rating for a movie
 * @parma {httpRequest} req
 * @param {httpResponse} res
 */
exports.predictedRatingForMovie = function(req, res){
	// FIXME just use the DataFrame from the last run!
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
 * @parma {httpRequest} req
 * @param {httpResponse} res
 */
exports.movieID = function(req, res){
	if (new_user_recommendations_rating_title_and_count_DF) {
		var col2 = new_user_recommendations_rating_title_and_count_DF.col("title");
	    var testCol = col2.contains(req.query.movie);
	    var resultDF = new_user_recommendations_rating_title_and_count_DF.filter(testCol);
	    resultDF.take(10).then(function(r){
			console.log("from the DF: "+ JSON.stringify(r));
			var movies = []
			r.forEach(function(movie){
				var m = {};
				m.id = movie.get(movie.fieldIndex("id"));
		        m.title = movie.get(movie.fieldIndex("title"));
		        m.rating = movie.get(movie.fieldIndex("predicted_ratings"));
		        m.numberOfRatings = movie.get(movie.fieldIndex("ratings_count"));
				movies.push(m);
			});
			res.send( JSON.stringify(movies));
		}, failureExit);
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
	
};

/**
 * REST Service returns JSON
 * Returns for for the given id
 * @parma {httpRequest} req
 * @param {httpResponse} res
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
 * @parma {httpRequest} req
 * @param {httpResponse} res
 */
exports.rateMovie = function(req, res){
	console.log("rateMovie");
	if (new_user_recommendations_rating_title_and_count_DF /* top25Recommendation */) {
		//var rating = { "id":req.body.id, "rating":req.body.rating};
		userMovieRatingHash[req.query.id] = parseInt(req.query.rating);
	    // update the predicted ratings for this user
	    // top25Update will be broadcast over websocket
		if (rebuildingTop25 == 'complete') {
			rebuildingTop25 = 'inprogress';
			loadSavedData(function() {
				console.log("back from loadSavedData");
				rateMoviesForUser(function(){
					console.log("back from rateMoviesForUser");
					handleTop25Update(top25Recommendation);
				});
			});
			
		} else {
			console.log('rateMovie inprogress, so we will put on the mark as needed');
			rebuildingTop25 = 'needed';
		}
		handleTop25Update(top25Recommendation);
	    // send back userHash
	    res.send(JSON.stringify(userMovieRatingHash));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};

/**
 * REST Service returns JSON
 * Rebuild the models and datasets
 * @parma {httpRequest} req
 * @param {httpResponse} res
 */
exports.rebuild = function(req, res){
	if (top25Recommendation) {
		rebuildingTop25 = 'needed';
		movie_recommender_init();	
	}
	res.send( JSON.stringify({"message": "Still building modles, try again later."}));
};

/**
 * Triggets the update of top 25 movies predictions. If we have a top 25 update in progess 
 * we don'e want to start another.
 * @param updatedTop25
 * @param fromSearch
 */
function handleTop25Update(updatedTop25, fromSearch){
    console.log("handleTop25Update: from search  " + fromSearch);
	if (new_user_recommendations_rating_title_and_count_DF) {
//		var col2 = new_user_recommendations_rating_title_and_count_DF.col("title");
//	    var testCol = col2.contains(req.query.movie);
		refreshMovieRatingsDF(function(df){
			 var resultDF = df.filter("ratings_count > 24");
			    getTop25FromDF(resultDF, 
			    /*resultDF.take(25).then(*/function(r){
					console.log("from the DF: "+ JSON.stringify(r));
					var movies = []
					r.forEach(function(movie){
						var m = {};
						m.id = movie.get(movie.fieldIndex("id"));
				        m.title = movie.get(movie.fieldIndex("title"));
				        m.rating = movie.get(movie.fieldIndex("predicted_ratings"));
				        m.numberOfRatings = movie.get(movie.fieldIndex("ratings_count"));
						movies.push(m);
					});
					if (dataCallback) {
				    	dataCallback(JSON.stringify({type:'top25Update', data: movies, original25: fromSearch || false}));
					}

		});
	   
	    }, function(){
	    	console.log("handleTop25Update DF error");
	    });
	}

}

function refreshMovieRatingsDF(callback) {
    new_user_recommendations_rating_title_and_count_DF = getSparkSessionForMovieSearch()
    	.read()
    	.parquet(new_user_recommendations_rating_title_and_count_DF_path);
    new_user_recommendations_rating_title_and_count_DF.take(1).then(function(r){
    	console.log("refreshMovieRatingsDF complete");
    	if(callback) {
    		callback(new_user_recommendations_rating_title_and_count_DF);
    	}
    })
}

var sparkSessionForMovieModel;

function getSparkSessionForModel() {
	if (sessionStopTimer) {
		timers.clearTimeout(sessionStopTimer);
		sessionStopTimer = null;
	}
	if (!sparkSessionForMovieModel) {
		spark = new eclairjs();
		sparkSessionForMovieModel = spark.sql.SparkSession.builder()
		.appName("Movie Recommender Express")
		.config("spark.executor.memory", "10g")
		.config("spark.driver.memory", "6g")
		//.config("spark.sql.warehouse.dir", warehouseLocation)
		//.enableHiveSupport()
		.getOrCreate();
		var sc = sparkSessionForMovieModel.sparkContext();
		configForSwift(sc);
	}
	
	return sparkSessionForMovieModel;
}

function stopSparkSessionForModel() {
	if (sparkSessionForMovieModel) {
		sparkSessionForMovieModel.stop().then(function() {
			sparkSessionForMovieModel = null
			getSparkSessionForModel();
			  });
	}
}

var sparkSessionForMovieSearch;

function getSparkSessionForMovieSearch() {
	if (!sparkSessionForMovieSearch) {
		spark2 = new eclairjs();
		sparkSessionForMovieSearch = spark2.sql.SparkSession.builder()
		.appName("Movie Recommender Rating Search from NodeJS Express")
		//.config("spark.sql.warehouse.dir", warehouseLocation)
		//.enableHiveSupport()
		.getOrCreate();
		var sc = sparkSessionForMovieSearch.sparkContext();
		configForSwift(sc);
		
	}
	
	return sparkSessionForMovieSearch;
	
}

function stopSparkSessionForMovieSearch() {
	if (sparkSessionForMovieSearch) {
		sparkSessionForMovieSearch.stop().then(function() {
			sparkSessionForMovieSearch = null
			getSparkSessionForMovieSearch();
			  });
	}
}

/**
 * Start sending data updates to node server so it can farm out to clients attatched to websocket
 * @param {function} cb
 */
exports.startUpdates = function(cb) {
    dataCallback = cb;
};

/**
 * Display movie recommender search
 * @param {httpRequest} req
 * @param {httpResponce} res
 */
exports.rate = function(req, res){
  res.render('movie_recommender/rate', { title: 'Movie Recommender' });
};

function configForSwift(sc){
	var swift;
	if(process.env.VCAP_SERVICES) {
	    var vcap = JSON.parse(process.env.VCAP_SERVICES);   
	    if(vcap['Object-Storage']) {
	    	swift = vcap['Object-Storage'][0]; 
	
	    	var prefix = "fs.swift2d.service.softlayer.";
	
	    	sc.setHadoopConfiguration("fs.swift2d.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem"); 
	    	sc.setHadoopConfiguration(prefix+"auth.url", swift.credentials.auth_url + "/v3/auth/tokens");
	    	sc.setHadoopConfiguration(prefix+"tenant", swift.credentials.projectId);
	    	sc.setHadoopConfiguration(prefix+"public", true);
	    	sc.setHadoopConfiguration(prefix+"username", swift.credentials.userId);
	    	sc.setHadoopConfiguration(prefix+"password", swift.credentials.password);
	    	sc.setHadoopConfiguration(prefix+"region", swift.credentials.region);
	    	sc.setHadoopConfiguration(prefix+"auth.method", "keystoneV3");

	    }
	    console.log("##Starting spark VCAP:" + JSON.stringify(vcap));
	}
}

function loadSavedData(callback) {
	/*
	 * Start a Spark session for the model.
	 */
	
	var sparkSession = getSparkSessionForModel();
	var sc = sparkSession.sparkContext();
	configForSwift(sc);
	var sparkSessionForMovieRatingSearch = getSparkSessionForMovieSearch();
	
	 /* 
     * First try to load saved model and RDD's that have been computed in the passed
     */

    complete_ratings_data = sc.objectFile(complete_ratings_data_path); 
    predictionModleValuesDF = sparkSession.read().parquet(predictionModleValuesDFPath);
    complete_movies_titlesDF = sparkSession.read().parquet(complete_movies_titlesDFPath);
    complete_movies_data = sc.objectFile(complete_movies_data_path); 
    complete_movies_titles = spark.rdd.PairRDD.fromRDD(sc.objectFile(complete_movies_titles_path));
    movie_rating_counts_RDD = spark.rdd.PairRDD.fromRDD(sc.objectFile(movie_rating_counts_RDD_path));
    new_user_recommendations_rating_title_and_count_DF_filtered = sparkSession.read().parquet(new_user_recommendations_rating_title_and_count_DF_filtered_path);
   // new_user_recommendations_rating_title_and_count_DF = sparkSessionForMovieRatingSearch.read().json(new_user_recommendations_rating_title_and_count_DF_path);

    /*
     * We need to do a take, to actually determine if the RDD's and models where loaded.
     */
    Promise.all([
                 complete_ratings_data.take(1), 
                 predictionModleValuesDF.take(1), 
                 complete_movies_titlesDF.take(1), 
                 complete_movies_data.take(1),
                 complete_movies_titles.take(1),
                 movie_rating_counts_RDD.take(1),
                 new_user_recommendations_rating_title_and_count_DF_filtered.take(1)
                // new_user_recommendations_rating_title_and_count_DF.take(1)
                 ]).then(function(resluts){
                     /*
                      * The saved RDDs and model were loaded, so we can reusse them
                      */
        console.log(' All saved data loaded');
        var row = resluts[1][0];
        best_rank = row.get(row.fieldIndex('best_rank'));
        iterations = row.get(row.fieldIndex('iterations'));
        regularization_parameter = row.get(row.fieldIndex('regularization_parameter'));
        blocks = row.get(row.fieldIndex('blocks'));
        seed = row.get(row.fieldIndex('seed'));
        // load the model
        new_ratings_model = spark.mllib.recommendation.MatrixFactorizationModel.load(sc, new_ratings_model_path);
        callback(); // FIXME we should use a promise
    }, function(err){
        /*
         * RDDs and models do not exist so we need to build them
         */
        console.log('No saved data rebuilding models and data, this could take a while...');
        movie_recommender_init();
        callback(); // FIXME we should use a promise
        
    });

}

function init() {

	// Allow bypass of using saved data for debug/dev sake.
	if (use_saved_data && use_saved_data.toUpperCase() == "TRUE") {
		loadSavedData(function(){
			 getTop25FromDF(new_user_recommendations_rating_title_and_count_DF_filtered, function(top_movies) {
			        top25Recommendation = top_movies;
			        refreshMovieRatingsDF(function(){
			        	rebuildingTop25 = 'complete';
				        handleTop25Update(top_movies);
			        });
			        
			 });
		});

	} else {
	    movie_recommender_init();
	}

}

init();
