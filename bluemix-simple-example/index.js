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

// use express for a webserver
var express = require('express');

var port = process.env.VCAP_APP_PORT || 5000;
var host = process.env.VCAP_APP_HOST || 'localhost';

// setup the express server
var app = express();
app.use(express.static('public'));

var server = app.listen(port, host, function () {
});

// eclairjs
var eclairjs = require('eclairjs');
var sc;

// our main entry point
app.get('/do', function (req, res) {
  sc = new eclairjs.SparkContext("local[*]", "Simple Spark Program");

  var rdd = sc.parallelize([1.10, 2.2, 3.3, 4.4]);

  var rdd2 = rdd.map(function(num) {
    return num*2;
  });

  rdd2.collect().then(function(results) {
    res.json({result: results});
    sc.stop();
  }).catch(function(err) {
    console.error("error", err);
    res.status(500).send({error: err.msg});
    sc.stop();
  });
});

// stop spark when we stop the node program
process.on('SIGTERM', stop);
process.on('SIGINT', stop);

function exit() {
  process.exit(0);
}

function stop(e) {
  if (e) {
    console.log('Error:', e);
  }

  if (sc) {
    sc.stop().then(exit).catch(exit);
  }
}