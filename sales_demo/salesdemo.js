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

var eclairjs = require('eclairjs');
var spark = new eclairjs();
var session = spark.sql.SparkSession.builder()                                  
 .appName("Retail Sales Demo")                                                        
 .getOrCreate();  

var ssc = new spark.streaming.StreamingContext(session, new spark.streaming.Duration(250));

var avgPriceData = {};

var allDatesInput = session.read().parquet(
  process.env.DATES_DATA || __dirname + "/data/models/dates/V11_arr"
);

//do a count to load these into memory
allDatesInput.count().then(function(c){
    console.log("allDatesInput count = " + c);
});

var batchTlogLines = session.read().textFile(
  process.env.BATCH_DATA || __dirname + "/data/batch/*"
);

var avgPrice = session.read().parquet(
  process.env.PRICE_DATA || __dirname + "/data/models/avgsaleprice.parquet"
);

avgPrice.collect().then(function(c) {
  c.forEach(function(d) {
    var dateStr = d.get(0);
    //var parts = dateStr.split('-');
    //var date = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]))
    //var key = date.getTime();
    //avgPriceData[key] = d.get(1);
    avgPriceData[dateStr] = d.get(1);
  });
  
  //console.log(avgPriceData);
});

var DataTypes = spark.sql.types.DataTypes

var fields = [];
fields.push(DataTypes.createStructField("product_id", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("store_id", DataTypes.StringType, true));
fields.push(DataTypes.createStructField("date", DataTypes.DateType, true));
fields.push(DataTypes.createStructField("time", DataTypes.TimestampType, true));
fields.push(DataTypes.createStructField("units", DataTypes.DoubleType, true));
fields.push(DataTypes.createStructField("price", DataTypes.DoubleType, true));
fields.push(DataTypes.createStructField("type", DataTypes.StringType, true));

var schema = DataTypes.createStructType(fields);

var kafkaHost = process.env.KAFKA_HOST || "172.16.20.132";

var maxPrice = 2199.98;

//Create Linear Regresssion Model from static batch data.
function createModel(rdd) {
    var trainingRDD =  rdd.map(function(line, Tuple, SqlDate, SqlTimestamp) {
        var v = line.split("|")
        //product, location, date, time, units sold, price
        //return new Tuple(v[7], v[2], v[4], v[5], parseFloat(v[8]), parseFloat(v[10]), v[6]);
        return [v[7], v[2], v[4], v[5], parseFloat(v[8]), parseFloat(v[10]), v[6]];
    }, []).filter(function(x) {
        //filter out 0 units sold and 0 price
        return (x[5] > 0 && x[4] > 0 && x[6] == "SALE");
    }).map(function(t, RowFactory, SqlDate, SqlTimestamp) {
        var parts = t[2].match(/(\d+)/g);
        var date = new Date(parts[0], parts[1]-1, parts[2]);
        var time = new Date(Date.parse(t[2]+" "+t[3]));
        var row = RowFactory.create([
            t[0], 
            t[1], 
            new SqlDate(date.getTime()),
            new SqlTimestamp(time.getTime()), 
            parseFloat(t[4]), 
            parseFloat(t[5]), 
            t[6]]);
        return row;
    }, [spark.sql.RowFactory,spark.sql.SqlDate,spark.sql.SqlTimestamp]);

    var df = session.createDataFrame(trainingRDD, schema);
    var df2 = df.join(allDatesInput, "date");
    var df3 = df2.groupBy("date", "values").agg({"price": "sum", "units": "sum"});

    var rddLP = df3.map(function(row, maxPrice, Vectors, LabeledPoint) {
        var dateData = row.getList(1);

        var priceTotal = row.get(2);
        var unitsTotal = row.get(3);

        var price = priceTotal / unitsTotal;
        var featuresArray = [Math.log(maxPrice / price)];
        dateData.forEach(function(val) {
          featuresArray.push(val);
        });

        //print(featuresArray);
        //print(featuresArray.length);
        //var features = Vectors.dense([Math.log(maxPrice / price)].concat(dateData))
        var features = Vectors.dense(featuresArray);
        //print("features: " + features);

        return new LabeledPoint(
            Math.log(unitsTotal+1.0), 
            features
        );
    }, [maxPrice, spark.mllib.linalg.Vectors, spark.mllib.regression.LabeledPoint]);

    var model = spark.mllib.regression.LinearRegressionWithSGD.train(rddLP, 100);
    return model;
}

function startKafkaStream(cb) {
  console.log("Using kafakHost: " + kafkaHost);

  //Use Kafka Receiver
  return spark.streaming.kafka.KafkaUtils.createStream(
    ssc, kafkaHost+":2181", "salesdemo-group", {"tlog": 1}
  ).map(function(t, RowFactory, SqlDate, SqlTimestamp) {
    var line = t._2().trim();
    var v = line.split("|");
    var parts = v[4].match(/(\d+)/g);
    var date = new Date(parts[0], parts[1]-1, parts[2]);
    var time = new Date(Date.parse(v[4]+" "+v[5]));
    var row = RowFactory.create([v[7], 
        v[2], 
        new SqlDate(date.getTime()), 
        new SqlTimestamp(time.getTime()), 
        parseFloat(v[8]), 
        parseFloat(v[10]), 
        v[6]]);

    return row;
  }, [spark.sql.RowFactory, spark.sql.SqlDate, spark.sql.SqlTimestamp]).filter(function(row) {
    //filter out 0 units sold and 0 price
    return (row.get(5) > 0 && row.get(4) > 0 && row.get(6) == "SALE");
  }).foreachRDD(
    function(rdd, sqlContext, schema, allDatesInput) {
      print("1");
      var df = sqlContext.createDataFrame(rdd, schema);
      print("2");
      var df2 = df.join(allDatesInput, "date");
      print("3");
      var rows = df2.collect();
      print("4");

      var data = [];
      rows.forEach(function(row) {
        data.push(JSON.stringify({
          "price": row.get(5),
          "sales": row.get(4),
          "date": row.getDate(0).getJavaObject().getTime(),
          "time": row.getTimestamp(3).getTime(),
          "dateData": row.get(7)
        }));
      });
      print("5");

      return data;
    }, 
    [sqlContext, schema, allDatesInput],
    function(result) {
      if (result) {
        if(result && result.length > 0) {
          result.forEach(function(r) {
            cb(JSON.parse(r));
          });
        }
      } 
    }
  ).then(function() {
    ssc.start();
  });
}

//function predict(date, totalPrice, totalSales, dateData, model) {
function predict(date, dateData, model) {
  return new Promise(function(resolve, reject) {
    var d = new Date(date);
    var month = d.getUTCMonth() + 1
    if(month < 10) {
      month = "0" + month;
    }
    var day = d.getUTCDate();
    if(day < 10) {
      day = "0" + day;
    }
    var key = d.getUTCFullYear() + "-" + month + "-" + day;
    console.log("key = " + key);
    var price = avgPriceData[key];
    console.log("price = " + price);
    var features = [Math.log(maxPrice / price)].concat(dateData);

    console.log("features = " + features);

    model.predict(spark.mllib.linalg.Vectors.dense(features)).then(function(sales) {
      resolve({
        //"actualSales": units,
        "predictedSales": Math.exp(sales) - 1,
        "price": price,
        "date": date
      });
    }).catch(reject);
  });
};

var model;
var stream;

var startStream = function(dataCallback) {
    if (!stream) {
        model = createModel(batchTlogLines);

        var dummyvectors = spark.mllib.linalg.Vectors.dense([0.82976249729615,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0]);

        model.predict(dummyvectors).then(function() {
          console.log("model done");
          startKafkaStream(dataCallback);
        }).catch(console.error);
    }
}

function RetailSalesDemo() {
}

RetailSalesDemo.prototype.start = function(uiCallback, dataCallback) {
  startStream(dataCallback);
  if (uiCallback) uiCallback();
}

RetailSalesDemo.prototype.stop = function(callback) {
  if (session) {
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
            session.stop().then(callback).catch(callback);
        });
    } else {
        session.stop().then(callback).catch(callback);
    }
  }
}

//RetailSalesDemo.prototype.predict = function(date, totalPrice, totalSales, dateData) {
RetailSalesDemo.prototype.predict = function(date, dateData) {
  //return predict(date, totalPrice, totalSales, dateData,  model);
  return predict(date, dateData,  model);
}

RetailSalesDemo.prototype.query = function(sql) {
  return sqlContext.sql(sql);
}

module.exports = new RetailSalesDemo();
//}()
