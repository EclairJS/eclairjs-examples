/*
 * Copyright 2015 IBM Corp.
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
var WebSocketServer = require('ws').Server;

var app = express();
app.use(express.static('public'));
app.use(express.static(__dirname + '/public'));

var port = process.env.VCAP_APP_PORT || 3000;
var host = process.env.VCAP_APP_HOST || 'localhost';

var server = app.listen(port, host, function () {
  console.log('listening on *:'+port);
});

var wss = new WebSocketServer({
  server: server
});

function sendToSocket(data) {
  wss.clients.forEach(function(client) {
    try {
      //console.log("Sending predict data to browser: ",predictData);
      client.send(JSON.stringify(data));
    } catch (e) {
    }
  })
}

var airlineDemo = require('./airline.js');

var registeredAirports = [];

wss.on('connection', function(ws) {
  ws.on('message', function(message) {
    console.log("*******",message);

    var msg = JSON.parse(message);

    if (msg && msg.registerAirport) {
      // we want to register for flight updates airport
      registeredAirports.push(msg.registerAirport);
      var airportCode = msg.registerAirport;
      // see if anything has accumulated for this ariport and push to browser if so
      if (flightData[airportCode]) {
        updateAirportCharts(airportCode, JSON.stringify(flightData[airportCode]));
        flightData[airportCode] = [];
      }
    } else if (msg && msg.unregisterAirport) {
      // unregister an airport for flight updates
      registeredAirports.splice(registeredAirports.indexOf(msg.unregisterAirport),1);
    }

    console.log("popups are open for airports: ",registeredAirports.toString());
  });
});

app.get('/getCarriers', function (request, response) {
  var airportCode = request.query.airport;
  console.log("getCarriers for: ",airportCode);
  try {
    // carriers will be a sql.Dataset
    var carriers = airlineDemo.query("SELECT DISTINCT carrier FROM flightstoday WHERE origin='"+airportCode+"'");
    // the result from collect is an array of sql.Row types
    carriers.collect().then(function(result){
      var d = [];
      result.forEach(function(item) {
        d.push({carrier: item.get(0)});
      });
      console.log('distinct carriers for ',airportCode,': ',JSON.stringify(result));
      response.json(d);
    });
  } catch (e) {
    console.log("e", e)
  }
});

app.get('/getSchedule', function (request, response) {
    var airportCode = request.query.airport;
    var carrier = request.query.carrier;
    try {
    // flightsToday will be a Dataset
    var flightsToday = airlineDemo.query("SELECT flight_num,destination FROM flightstoday WHERE origin='" + 
        airportCode + "' AND carrier='" + carrier + "'");
    // the result from collect is an array of sql.Row types
    flightsToday.collect().then(function(result){
      var d = [];
      result.forEach(function(item) {
        d.push({flight_num: item.get(0), destination: item.get(1)});
      });
      response.json(d);
    });
  } catch (e) {
    console.log("e", e)
  } 
});

// start the demo
airlineDemo.start(function(data){
    handleStreamingData(data);
});

// Save flight data based on airportCode
var flightData = {};
var ACCUMULATE_THRESHOLD = 300;

function handleStreamingData(data) {
    //console.log("Handling streaming data: ",data);

    var airportCode = data.origin;
    // If we have flight data for a new airport add it to the data map.
    if (!flightData[airportCode]) {
        flightData[airportCode] = [];
    }
    flightData[airportCode].push(data);

    // For every X number of records collected broadcast the new flight data if the airport
    // has been registered.  Then start over with next X number of records that come in. If airport
    // has not been registered as "open and showing" then just accumlate so it has data if/when
    // it does get opened.
    if (registeredAirports.indexOf(airportCode) >= 0 && flightData[airportCode].length >= ACCUMULATE_THRESHOLD) {
        updateAirportCharts(airportCode, JSON.stringify(flightData[airportCode]));
        flightData[airportCode] = [];
    }
};

function updateAirportCharts(airportCode, data) {
  console.log("Sending flight data to browser for airportCode: ",airportCode);
  sendToSocket({type: 'flightdata', airportCode: airportCode, data: data});
};

// stop spark streaming when we stop the node program
process.on('SIGTERM', function () {
  airlineDemo.stop(function() {
    console.log('SIGTERM - stream has been stopped');
    process.exit(0);
  });
});

process.on('SIGINT', function () {
  airlineDemo.stop(function() {
    console.log('SIGINT - stream has been stopped');
    process.exit(0);
  });
});
