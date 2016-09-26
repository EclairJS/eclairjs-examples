var eclairjs = require('eclairjs');
var spark = new eclairjs();

//var spark = require('/Users/billreed/eclairjs_dev/eclairjs-node/lib/index.js');
//var spark = require('/Users/jbarbetta/Work/gitProjects/eclairjs/eclairjs-node/lib/index.js');

//exports.list = {
		exports.word_count = function(req, res){
			var sc = new spark.SparkContext("local[*]", "foo");
		
			var file = '/Users/billreed/eclairjs_dev/eclairjs-node/examples/dream.txt';
            //var file = '/Users/jbarbetta/Work/gitProjects/eclairjs/eclairjs-node/lib/index.js';
		
			// first argument is a new filename
			if (process.argv.length > 2) {
			  file = process.argv[2];
			}
		
			var rdd = sc.textFile(file);
		
			var rdd2 = rdd.flatMap(function(sentence) {
			  return sentence.split(" ");
			});
		
			var rdd3 = rdd2.filter(function(word) {
			  return word.trim().length > 0;
			});
		
			var rdd4 = rdd3.mapToPair(function(word, Tuple2) {
			  return new Tuple2(word.toLowerCase(), 1);
			}, [spark.Tuple2]);
		
			var rdd5 = rdd4.reduceByKey(function(value1, value2) {
			  return value1 + value2;
			});
		
			var rdd6 = rdd5.mapToPair(function(tuple, Tuple2) {
			  return new Tuple2(tuple._2() + 0.0, tuple._1());
			}, [spark.Tuple2]);
		
			var rdd7 = rdd6.sortByKey(false);
		
			rdd7.take(10).then(function(val) {
			  //console.log("Success:", val);
			  res.send("Success:", val);
			  sc.stop().then(function() {
			   // process.exit();
			  }, function(e) {
				    //console.log(e);
				    res.send(e);
				    //process.exit();
				    sc.stop().then(function() {
					   // process.exit();
					  });
				  }); /*.catch(function(e) {
			    console.log(e);
			    process.exit();
			  });*/
		
			}, function(err) {
				  console.log("Error:", err);
				  res.send(err);
				  sc.stop().then(function() {
				    //process.exit();
				  }); /*.catch(function(err) {
			  console.log("Error:", err);
			  sc.stop().then(function() {
			    process.exit();
			  });*/
			});
		 // res.send("respond with a resource");
		};
		
		exports.users = function(req, res) {
			 res.send("respond with a resource");
		};
