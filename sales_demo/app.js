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

var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var WebSocketServer = require('ws').Server;

var interval;

var wss;

var demo = require('./salesdemo.js');

// the date we want to show in the UI
//var theDate = new Date(Date.parse('2013-07-18'));
var theDate = 0;

// 24 hours
function initHourlyData(date) {
  var data = [];
  for(var i=0; i < 24; i++) {
    data[i] = {time: new Date(date).setHours(i), sales: 0};
  }

  // Be careful - index is 0-23 for the 24 hours in day.
  data[24] = {predictedSales: 0}

  return data;
}

var dailyData = initHourlyData(new Date().getTime());

function updateBarGraph(dd) {
  wss.clients.forEach(function(client) {
    try {
      //console.log("Sending hourly data to browser: ",dd);
      client.send(JSON.stringify({type: 'hourlydata', data: dd}));
    } catch (e) {
    }
  })
}

function updateLineGraph(predictData) {
  wss.clients.forEach(function(client) {
    try {
      //console.log("Sending predict data to browser: ",predictData);
      client.send(JSON.stringify({type: 'predictdata', data: predictData}));
    } catch (e) {
    }
  })
}

function formatDate(d) {
  var yearStr = d.getFullYear();
  var monthStr = (d.getMonth() + 1) + "";
  // Be careful here!  Date.getDate() returns the day of the month 
  // from 1 to 31 so we don't need to add one!
  var dayStr = (d.getDate()) + "";

  monthStr = (monthStr.length == 1) ? "0" + monthStr : monthStr;
  dayStr = (dayStr.length == 1) ? "0" + dayStr : dayStr;

  return yearStr+"-"+monthStr+"-"+dayStr;
}

function random() {
    return Math.round(Math.random()*100);
};


//{key: date, val: [{time: _, sales: _, price: _, dateData: _}]}
var streaming_data = {};

function enqueue(item) {
  var key = item.date;
  if(!(key in streaming_data)) {
    streaming_data[key] = [];
  }

  streaming_data[key].push({
    time: item.time,
    sales: item.sales,
    price: item.price//,
    //dateData: item.dateData
  });

  return streaming_data[key];
}

function predict_data(date) {
  var arr = streaming_data[date];
  if(arr == undefined) {
    console.log("date not found: " + date);
    return null;
  }

  var totalSales = arr.reduce(function(x, y) {
    return x + y.sales;
  }, 0);

  var totalPrice = arr.reduce(function(x, y) {
    return x + y.price;
  }, 0);

  return {
    totalSales: totalSales, 
    totalPrice: totalPrice
  };
}

var todaysPredictedSales = null;

// handles hourly data
function handleHourlyData(item) {
  //console.log("handleHourlyData: ", item);

  var currentData = enqueue(item);

  // item.date is a unix timestamp
  var date = new Date(item.date);

  //var time = new Date(item.time*1000);
  var time = new Date(item.time);
  //console.log("time = " + time);

  var newDay = false;

  //console.log(time + " sale: " + item.sales);
  //if the dates don't match we've started a new day of data.
  //reset the bar graph and get predictions for previous day.
  if(theDate != item.date) {
    if(todaysPredictedSales) {
        var pd = predict_data(theDate);
        updateLineGraph({
          actualSales: pd.totalSales,
          predictedSales: todaysPredictedSales.predictedSales,
          price: todaysPredictedSales.price,
          date: theDate
        });
    }

    if(theDate != 0) {
      newDay = true;
    }

    var dd = initHourlyData(item.date);
    updateBarGraph(dd);
    dailyData = dd;
    theDate = item.date;
    //console.log("new day theDate: ",new Date(theDate));
    //console.log("new day item.date: ",new Date(item.date));
    //if our date is 0 it means we just started no prediction data.
    //var p = demo.predict(item.date, item.dateData);
    var p = demo.predict(item.date);
    p.then(function(val) {
      console.log("got new prediction: ", JSON.stringify(val));
      todaysPredictedSales = val;
      dailyData[24] = {predictedSales: val.predictedSales}

    }).catch(function(err) { console.log("error = " + err); });

  }

  if(!newDay) {
    dailyData[time.getHours()].sales = currentData.reduce(function(x, y) {
      return x + y.sales;
    }, 0) + item.sales;

    for(var i=1; i<=time.getHours(); i++) {
      if(dailyData[i].sales == 0)
        dailyData[i].sales = dailyData[i-1].sales;
    }
  }

  /*
  // If we change buckets then hourly sales will be zero.  The hour before will have
  // accumlated total for the day thus far, so init the current hourly bucket with that.
   if (dailyData[time.getHours()].sales == 0 && time.getHours() > 0) {
    dailyData[time.getHours()].sales = dailyData[time.getHours() - 1].sales;
  }
  // Add the new sales that just came in to the current hourly bucket.
  dailyData[time.getHours()].sales += item.sales;
  */

  updateBarGraph(dailyData);
}

var app = express();

app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.set('port', (process.env.VCAP_APP_PORT || 5000));
app.set('host', (process.env.VCAP_APP_HOST || 'localhost'));

/*
app.get('/', function(req, res) {
  res.send("Hello World");
});
*/

//app.use(express.static(__dirname + '/public'));


// Fake data:

/*
setInterval(function() {
  var unixdate = theDate.getTime()/1000;

  var data = [];
  data.push({date: unixdate, time: new Date(Date.parse('2013-08-17 10:23:30')).getTime()/1000, sales: 4});
  handleHourlyData(data);
}, 15000);
*/

// start the demo
demo.start(function(){
}, function(data) {
  // we got data from the demo data
  handleHourlyData(data);
});

var server = app.listen(
  process.env.VCAP_APP_PORT || 5000,
  process.env.VCAP_APP_HOST || 'localhost', 
  function() {
    console.log("demo running on port " + app.get('port'));
  }
);

wss = new WebSocketServer({
  server: server
});

wss.on('connection', function(ws) {
  gWS = ws;

  ws.on('message', function(message) {
    var msg = JSON.parse(message);

    //console.log("*******",message);

    if (msg && msg.registerForStore) {
      // we want to register for a certain store
    } else if (msg && msg.unregisterForStore) {
      // unregister for a store
    }
  });
});




// stop spark streaming when we stop the node program
process.on('SIGTERM', function () {
  demo.stop(function() {
    console.log('SIGTERM - stream has been stopped');
    process.exit(0);
  });
});

process.on('SIGINT', function () {
  demo.stop(function() {
    console.log('SIGINT - stream has been stopped');
    process.exit(0);
  });
});

