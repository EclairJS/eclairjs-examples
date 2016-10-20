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
var WebSocketServer = require('ws').Server;

var port = 5000;
var host = 'localhost';

// setup the express server
var app = express();
app.use(express.static('public'));

var server = app.listen(port, host, function () {
});

var wss = new WebSocketServer({
  server: server
});

function sendToSocket(data) {
  wss.clients.forEach(function(client) {
    try {
      client.send(JSON.stringify(data));
    } catch (e) {
    }
  });
}

var netstream = require('./src/netestream');

var stream1 = new netstream(4001, [1,2,3]);
var stream2 = new netstream(4002, [4,5,6]);

var sparkstream = require('./src/sparkstream');

var sparkstream1 = new sparkstream("Stream 1", 4001, function(data) {
  sendToSocket({stream: 1, data: data})
});

var sparkstream2 = new sparkstream("Stream 2", 4002, function(data) {
  sendToSocket({stream: 2, data: data})
});

// stop spark when we stop the node program
process.on('SIGTERM', stop);
process.on('SIGINT', stop);

function exit() {
  process.exit(0);
}

function stop() {
  var p = [];
  p.push(sparkstream1.stop());
  p.push(stream1.stop());
  p.push(sparkstream2.stop());
  p.push(stream2.stop());

  Promise.all(p).then(exit).catch(exit);
}