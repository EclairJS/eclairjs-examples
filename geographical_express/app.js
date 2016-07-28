
/**
 * Module dependencies.
 */

var express = require('express')
  , routes = require('./routes')
  , geographical = require('./routes/geographical/maps')
  , http = require('http')
  , path = require('path')
  , WebSocketServer = require('ws').Server;

var app = express();

var wss;

// all environments
app.set('port',  process.env.VCAP_APP_PORT || 3000);
app.set('host',  process.env.VCAP_APP_HOST || 'localhost');
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
  app.use(express.errorHandler());
}

app.get('/', routes.index);
// REST services for geographical
app.get('/geographical/rest/housingAvgs', geographical.housingAvgs);
app.get('/geographical/rest/rebuildHousingAvgs', geographical.rebuildHousingAvgs);
// web UI for geographical
app.get('/geographical/maps', geographical.maps);

var server = http.createServer(app).listen(
  app.get('port'), 
  app.get('host'), 
  function(){
    console.log('Express server listening on port ' + app.get('port'));
  }
);

