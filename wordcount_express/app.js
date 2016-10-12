/**
 * app.js - Main NodeJS Express Application Controller
 */

// Module dependencies.
var express = require('express'),
    path = require('path'),
    pug = require('pug'),
    home = require('./routes/home');

var WebSocketServer = require('ws').Server;

var app = express();

// Setup the application's environment.
app.set('port',  process.env._EJS_APP_PORT || 3000);
app.set('host',  process.env.EJS_APP_HOST || 'localhost');
app.set('view engine', 'pug');
app.set('views', __dirname + '/views');
app.use(express.static(path.join(__dirname, 'public')));

// Route all GET requests to our public static index.html page
app.get('/', home.index);

// Start listening for requests
var server = app.listen(app.get('port'), app.get('host'), function(){
    console.log('Express server listening on port ' + app.get('port'));
  }
);

var wss = new WebSocketServer({
  server: server
});

wss.on('connection', function(ws) {
  ws.on('message', function(message) {
    console.log("*******",message);
    var msg = JSON.parse(message);
    if (msg && msg.startCount) {
        startCount();
    }
  });
});

var count = require('./count.js');
function startCount() {
    var file = './data/dream.txt';
    count.start(file, function(rawdata){
        // Recall raw data from EclaisJS is Tuple2[] with {"0":count, "1":word}.
        // Convert to something the UI can easily use.
        //console.log("rawdata recieved from ejs: ",JSON.stringify(rawdata));
        var results = [];
        rawdata.forEach(function(result){results.push({count:result[0], word:result[1]})});
        wss.clients.forEach(function(client) {
            try {
                // Send the results to the browser
                client.send(JSON.stringify(results));
            } catch (e) {
                console.log(e);
            }
        });
    });
};

// stop spark  when we stop the node program
process.on('SIGTERM', function () {
  count.stop(function() {
    console.log('SIGTERM - stream has been stopped');
    process.exit(0);
  });
});

process.on('SIGINT', function () {
  count.stop(function() {
    console.log('SIGINT - stream has been stopped');
    process.exit(0);
  });
});

