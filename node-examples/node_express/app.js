
/**
 * Module dependencies.
 */

var express = require('express')
  , routes = require('./routes')
  , user = require('./routes/user')
  , movie_recommender = require('./routes/movie_recommender/movie_recommender')
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
app.get('/users', user.users);
app.get('/word_count', user.word_count);
// REST services for geographical
app.get('/geographical/rest/housingAvgs', geographical.housingAvgs);
// web UI for geographical
app.get('/geographical/maps', geographical.maps);
//REST services for movie_recommender
app.get('/movie_recommender/rest/top25', movie_recommender.top25);
app.get('/movie_recommender/rest/predictedRatingForMovie', movie_recommender.predictedRatingForMovie);
app.get('/movie_recommender/rest/movieID', movie_recommender.movieID);
app.get('/movie_recommender/rest/movieTitle', movie_recommender.movieTitle);
app.post('/movie_recommender/rest/rateMovie', movie_recommender.rateMovie);
// web UI for movie_recommender
app.get('/movie_recommender/rate', movie_recommender.rate);

var server = http.createServer(app).listen(
  app.get('port'), 
  app.get('host'), 
  function(){
    console.log('Express server listening on port ' + app.get('port'));
  }
);

wss = new WebSocketServer({
  server: server
});

wss.on('connection', function(ws) {
  ws.on('message', function(message) {
    var msg = JSON.parse(message);

    console.log("*******",message);

    // Not really using this now - could be anything really.
    if (msg && msg.blah) {
      // we want to register for top25 updates 
    }
  });
});

var currentTop25;
movie_recommender.startUpdates(function(data){
    // we got data from the movie_recommender service
    var data = JSON.parse(data);
    if (data && data.type === 'top25Update') {
        //console.log("Got top25 data from movie_recommender: ",JSON.stringify(data));
        // If we need to do anything with the data now is our chance.
        currentTop25 = data;  // Save off so if any new client connection is made we can send
        handleTop25Updates(data);
    }
});

function handleTop25Updates(data) {
    wss.clients.forEach(function(client){
        try {
            //console.log("Sending top25 data to browser: ",JSON.stringify(data));
            client.send(JSON.stringify(data));
        } catch(e) {
            console.log("error updating clients: ",e);
        }
    });
};
