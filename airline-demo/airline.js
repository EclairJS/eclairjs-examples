/*
 * Copyright 2016 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var spark = require('eclairjs');

var sparkMaster = process.env.SPARK_MASTER || "local[*]";
console.log("spark master = " + sparkMaster);
var sc = new spark.SparkContext(sparkMaster, "Airline Demo");
var sqlContext = new spark.sql.SQLContext(sc);
var ssc;

var DataTypes = spark.sql.types.DataTypes;

var fields = [];
fields.push(DataTypes.createStructField("origin", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("carrier", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("flight_num", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("destination", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("take_off_delay_mins", DataTypes.DoubleType, true));

var schema = DataTypes.createStructType(fields);

// rdu,aa,234,sfo,3
function startStream(cb) {
  ssc = new spark.streaming.StreamingContext(sc, new spark.streaming.Duration(2000));
  var kafkaHost = process.env.KAFKA_HOST || "localhost:2181"
  var dstream = spark.streaming.kafka.KafkaUtils
    .createDirectStream(ssc, {"metadata.broker.list": "kafka:9092"}, ["airline"])
  /*var dstream = spark.streaming.kafka.KafkaUtils
    .createStream(ssc, kafkaHost, "floyd", {"airline": 1})*/
    //.createStream(ssc, "10.11.19.101:2181", "floyd", "airline")
    .window(new spark.streaming.Duration(1000 * 60 * 15))
    .flatMap(function(chunk) {
      return chunk[1].split('\n');
    })
    .map(function(line) {
      var lineArr = line.split(",");
      var str = JSON.stringify({
        "origin": lineArr[16],
        "carrier": lineArr[8],
        "flight_num": lineArr[9],
        "destination": lineArr[17],
        //"take_off_delay_mins": parseInt(lineArr[15])
        "take_off_delay_mins": lineArr[15]
      })

      return str;
    });

  dstream.foreachRDD(
    function(rdd, sqlContext, schema) {
      print("on server");
      var df = sqlContext.read().json(rdd);
      var rows = df.collect();
      var data = [];
      rows.forEach(function(row) {
        data.push(JSON.stringify({
          "carrier": row.get(0),
          "destination": row.get(1),
          "flight_num": row.get(2),
          "origin": row.get(3),
          "take_off_delay_mins": parseInt(row.get(4))
        }));
      });
      return data;
    }, [sqlContext, schema],
    function(result) {
      if (result) {
        //console.log("Got result from foreachRDD: ",result);
        if (result && result.length > 0) {
          //console.log("Got result from foreachRDD at: ",new Date(Date.now()).toLocaleTimeString(), " with length: ",result.length);
          result.forEach(function(r) {
            cb(JSON.parse(r));
          });
          //cb(JSON.parse(result));
        }
      }
    }
  ).then(function() {
    ssc.start().catch(function(err) {
      console.log("error starting streaming context");
      console.log(err);
    })
  }).catch(function(err) {
    console.log("error starting stream");
    console.log(err);
  })
}

function getTodaysFlights() {
    var file = process.env.FLIGHT_DATA || 'file:/staticdata';
    console.log('Getting static data from file: ',file);

    var dfAllFlights = sqlContext.read().json(file);
    dfAllFlights.count().then(function(count){
        console.log('Num all US flights: ',count);
    }).catch(function(e) {
      console.log("fail", e)
    });

    var today = new Date();
    var month = today.getMonth()+1; // 0 indexed e.g. 0-11
    var day = today.getDate(); // 1 indexed e.g. 1-31

    var dfFlightsForToday = 
        dfAllFlights.filter("month='"+month+"' AND day='"+day+"'");
    dfFlightsForToday.count().then(function(count){
        console.log('Num all flights for today '+month+'-'+day+': ',JSON.stringify(count));
        dfFlightsForToday.registerTempTable('flightstoday').then(function(){
            console.log('Temptable flightstoday registered');
        });
    }).catch(function(e) {
      console.log("fail", e)
    });
}

function AirportDemo() {
}

AirportDemo.prototype.start = function(dataCallback) {
  startStream(dataCallback);
  getTodaysFlights();
}

AirportDemo.prototype.stop = function(callback) {
  if (sc) {
    console.log('stop - SparkContext exists');
    if (ssc) {
        console.log('stop - SparkStreamingContext exists');
        ssc.stop();
        ssc.awaitTerminationOrTimeout(5000).then(function() {
            sc.stop().then(callback).catch(callback);
            //callback();
        }).catch(function(err) {
            console.log("error stopping stream");
            //console.log(err);
            sc.stop().then(callback).catch(callback);
        });
    } else {
        sc.stop().then(callback).catch(callback);
    }
  }
}

AirportDemo.prototype.query = function(sql) {
  return sqlContext.sql(sql);
}

module.exports = new AirportDemo();
