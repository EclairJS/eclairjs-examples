var spark = require('eclairjs');

/*
 * Globals
 */

var housing_a_file_path = process.env.HOUSING_DIR + '/ss13husa.csv';
var housing_b_file_path = process.env.HOUSING_DIR + '/ss13husb.csv';
var states_file_path = process.env.HOUSING_DIR + '/states.csv';
var housing_avgs_by_state_df_json_file_path = process.env.HOUSING_DIR + '/housing_avgs_by_state_df_json';
var housing_avgs_by_state_results;

/*
 * Start a Spark session
 */

var sparkConf = new spark.SparkConf(false)
.set("spark.executor.memory", "10g")
.set("spark.driver.memory", "10g")
.setMaster(process.env.SPARK_MASTER || "local[*]")
.setAppName("geographical");

var sc = new spark.SparkContext(sparkConf);
var sqlContext = new spark.sql.SQLContext(sc);


/**
 * Builds the Dataframe that contains the housing averages
 */
function rebuild_housing_avgs_by_state_results() {
	/*
	 * load housing data.
	 */
	var housing_a_file_raw_data = sc.textFile(housing_a_file_path);
	housing_a_file_raw_data.take(1).then(function(val){
		var housing_a_file_data_header = val[0];
		console.log(housing_a_file_data_header);
		var housing_a_file_data = housing_a_file_raw_data
		.filter(function (line, housing_a_file_data_header) {
		    // filters out the header
		    return line != housing_a_file_data_header;
		}, [housing_a_file_data_header])
		.map(function (line, RowFactory) {
		    var tokens = line.split(",");
		    var values = [];
		    for (var i = 0; i < tokens.length; i++) {
		        if (i > 0) {
		            // RT (col 0 or A) is the only string in the dataset
		            values.push(parseInt(tokens[i]) | 0);
		        } else {
		            values.push(tokens[i]);
		        }
		    };
		    return RowFactory.create(values);
		}, [spark.sql.RowFactory] )
		.cache();
		housing_a_file_data.take(10).then(function(values){
			console.log("done " + JSON.stringify(values));
		});
		
		//Generate the schema
		var DataTypes = spark.sql.types.DataTypes;
		var fields = [];
		housing_a_file_data_header.split(',').forEach(function(colName){
		    if(colName == 'RT') {
		        fields.push(DataTypes.createStructField(colName, DataTypes.StringType, true));
		    } else {
		        fields.push(DataTypes.createStructField(colName, DataTypes.IntegerType, true));
		    }
		
		});
		
		var schema = DataTypes.createStructType(fields);
		//Apply the schema to the RDD.
		var housing_a_df = sqlContext.createDataFrame(housing_a_file_data, schema);
		
		var housing_b_file_raw_data = sc.textFile(housing_b_file_path);
		var housing_b_file_data = housing_b_file_raw_data
		    .filter(function (line, housing_a_file_data_header) {
		        // filters out the header
		        return line != housing_a_file_data_header; // should be the same header a a
		    }, [housing_a_file_data_header])
		    .map(function (line, RowFactory) {
		        var tokens = line.split(",");
		        var values = [];
		        for (var i = 0; i < tokens.length; i++) {
		            if (i > 0) {
		                // RT (col 0 or A) is the only string in the dataset
		                values.push(parseInt(tokens[i]) | 0);
		            } else {
		                values.push(tokens[i]);
		            }
		        };
		        return RowFactory.create(values);
		    }, [spark.sql.RowFactory])
		    .cache();
		var housing_b_df = sqlContext.createDataFrame(housing_b_file_data, schema);
		
		var states_file_raw_data = sc.textFile(states_file_path);
		states_file_raw_data.take(1).then(function(val){
			 var states_file_data_header = val[0];
			 var states_file_data = states_file_raw_data
		     .filter(function (line, states_file_data_header) {
		         // filters out the header
		         return line != states_file_data_header;
		     }, [states_file_data_header])
		     .map(function (line, RowFactory) {
		         var tokens = line.split(",");
		         var values = [];
		         for (var i = 0; i < tokens.length; i++) {
		             if (i < 1) {
		                 // col one is only int
		                 values.push(parseInt(tokens[i]) | 0);
		             } else {
		                 values.push(tokens[i]);
		             }
		         };
		         return RowFactory.create(values);
		     }, [spark.sql.RowFactory])
		     .cache();
		
		 //Generate the schema
		     var statesFields = [];
		     states_file_data_header.split(',').forEach(function(colName){
		         colName = colName.toUpperCase();
		         if(colName == 'ST') {
		             statesFields.push(DataTypes.createStructField(colName, DataTypes.IntegerType, true));
		         } else {
		             statesFields.push(DataTypes.createStructField(colName, DataTypes.StringType, true));
		         }
		
		     });
		
		     var statesSchema = DataTypes.createStructType(statesFields);
		     //Apply the schema to the RDD.
		     var states_df = sqlContext.createDataFrame(states_file_data, statesSchema).cache();
		     
		     var housing_df = housing_a_df.unionAll(housing_b_df).filter("ACR='1' OR ACR='2' OR ACR='3'");
		
		     var  housing_avgs_df = housing_df.groupBy("ST")
		         .agg({
		                 "VALP": "avg", // Property value
		                 "ACR":  "avg", // Lot size
		                 "ELEP": "avg", // Electric
		                 "GASP": "avg", // Gas
		                 "FULP": "avg", // Fuel cost(yearly cost for fuels other than gas and electricity)
		                 "WATP": "avg" // water
		            });
		
		     housing_avgs_by_state_df = housing_avgs_df.select(
		             housing_avgs_df.col("ST"),
		             housing_avgs_df.col("avg(VALP)").alias("AVG_VALP"),
		             housing_avgs_df.col("avg(ACR)").alias("AVG_ACR"),
		             housing_avgs_df.col("avg(ELEP)").alias("AVG_ELEP"),
		             housing_avgs_df.col("avg(GASP)").alias("AVG_GASP"),
		             housing_avgs_df.col("avg(FULP)").alias("AVG_FULP"),
		             housing_avgs_df.col("avg(WATP)").alias("AVG_WATP")
		         )
		         .join(states_df, 'ST').cache();
		     housing_avgs_by_state_df.write().mode('overwrite').json(housing_avgs_by_state_df_json_file_path); // save the DF as json for reuse when the server restarts
		     housing_avgs_by_state_df.collect().then(function(results){
		    	 housing_avgs_by_state_results = results;
		     });
		     /*
		     Show average utilises cost for AZ
		     */
		     
		    var result = housing_avgs_by_state_df.where('ST = 4');
		
		    result.take(1).then(function(res){
		         console.log("result: ",JSON.stringify(res));
		//         sc.stop().then(function() {
		// 		      process.exit();
		// 		    }, function(e) {
		// 		      console.log(e);
		// 		      process.exit();
		// 		    });
		     });
		});
	}, function(e){
		console.log(e)
	});
}

/*
 * Start by seeing if we have a saved dataframe we can reuse, by attempting to load and use it.
 */
var housing_avgs_by_state_df = sqlContext.read().json(housing_avgs_by_state_df_json_file_path);
housing_avgs_by_state_df.collect().then(function(results){
	/*
	 * We did have a saved dataframe, so lets reuse it.
	 */
	 housing_avgs_by_state_results = results;
	 console.log('results from saved DF housing_avgs_by_state_df_json_file_path loaded.');
},function(err) {
	/*
	 * No saved dataframe so we have to build it, this will take a little time.
	 */
	rebuild_housing_avgs_by_state_results();
});

/**
 * redirects the user to the maps
 * @param {httpRequest} req
 * @param {httpResponce} res
 */
exports.maps = function(req, res){
	res.render('geographical/maps', { title: 'Maps' });
};


/**
 * REST Service returns JSON
 * Updates the ratings movie ratings for this user and then re-run the movie recommender predictions
 * @param {httpRequest} req
 * @param {httpResponce} res
 */
exports.housingAvgs = function(req, res){
	if (housing_avgs_by_state_results) {
	    res.send(JSON.stringify(housing_avgs_by_state_results));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};
/**
* REST Service returns JSON, rebuilds the dataframe.
* @param {httpRequest} req
* @param {httpResponce} res
*/
exports.rebuildHousingAvgs = function(req, res){
	if (housing_avgs_by_state_results) {
		rebuild_housing_avgs_by_state_results();
	    res.send(JSON.stringify(housing_avgs_by_state_results));
	} else {
		res.send( JSON.stringify({"message": "Still building modles, try again later."}));
	}
};


