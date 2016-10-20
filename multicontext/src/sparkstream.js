var eclairjs = require('eclairjs');

function SparkStream(name, port, callback) {
  var spark = new eclairjs();
  var session = this.session = spark.sql.SparkSession.builder().appName(name).getOrCreate();
  var ssc = new spark.streaming.StreamingContext(session.sparkContext(), new spark.streaming.Duration(250));

  ssc.socketTextStream('localhost', port).foreachRDD(function(rdd) {
    return rdd.collect();
  }, [], function(res) {
    if (res && res.length > 0) {
      callback(res)
    }
  }).then(function () {
    ssc.start();
  }).catch(function(e) {
    console.log(e)
  });
}

SparkStream.prototype.stop = function() {
  return this.session.stop();
};

module.exports = SparkStream;