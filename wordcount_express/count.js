/**
 * count.js - EclairJS code that talks to Spark to run analytics (word count) on a text file.
 */

var eclairjs = require('eclairjs');
var spark = new eclairjs();

var sparkSession = spark.sql.SparkSession.builder()
  .appName("Word Count")
  .getOrCreate();


function startCount(file, callback) {
    var rdd = sparkSession.read().textFile(file).rdd();

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
      callback(val);
    }).catch(function(ex){console.log("An error was encountered: ",ex)});
}

// Create a wrapper class so we can interact with this module from NodeJS.
function Count() {
}

Count.prototype.start = function(file, callback, optionalInput) {
  // Do whatever with optionalInput here - can pass into startCount()
  // if desired or whatever from this point.
  console.log("Count.start received optionalInput param: ",optionalInput);

  startCount(file, callback);
}

Count.prototype.stop = function(callback) {
  if (sparkSession) {
    console.log('stop - SparkSession exists');
    sparkSession.stop().then(callback).catch(callback);
  }
}

module.exports = new Count();

