/**
 * http://usejsdoc.org/
 */

var port = location.port ? location.port : '80';
var socket = new WebSocket("ws://"+location.hostname+":"+port);
var thumbnails_off = true;

socket.onopen = function() {
    // heart beat, stops bluemix from closing connections
    window.setInterval(function() {
      socket.send(JSON.stringify({heartbeat: true}));
    }, 60000);
};

socket.onmessage = function(e) {
    if (e.data) {
        var data = JSON.parse(e.data);
        if (data.type === 'top25Update') {
            //console.log("Got top25 data.data: ",data.data);
            createTop25List(data.data, data.original25);
        }
    }
};

function init() {
    var element = document.getElementById("stars");
	element.innerHTML = "Enter title of movie you would like to rate: ";
	var input = document.createElement("input");
	input.type = "text";
	input.id = "moviesearch";
	input.onchange = movieSearch;
	element.appendChild(input);
	var movieSearchResults = document.createElement("div");
	movieSearchResults.id = "movieSearchResults";
	element.appendChild(movieSearchResults);
}


function restService (type, req, callback) {
    /* the AJAX request... */
    var oAjaxReq = new XMLHttpRequest();
    //var url = "/"+req;
    var url = req.indexOf("http") >= 0 ? req : "/"+req;
    // oAjaxReq.submittedData = oData;
    oAjaxReq.onload = function(e) {
    	var r = JSON.parse(e.target.response);
    	callback(r);
	};

      oAjaxReq.open(type, url);
      oAjaxReq.send(null);
}

function movieSearch(e) {
    // As per DF reset results with each new search instead of appending
    resetSearchResults("stars", "movieSearchResults");
    document.getElementById("top25").innerHTML = "";
	restService('get', "movie_recommender/rest/movieID?movie="+e.target.value+"&movieSearch=true", function(results){
		console.log(results);
		var movieSearchResults = document.getElementById("movieSearchResults");
		if (Array.isArray(results)) {
			results.forEach(function(movie) {
                var div = document.createElement("div");
                div.id = movie.id;
                div.classList.add("tn-wrapper");
                movieSearchResults.appendChild(div);
                var thumbnail = document.createElement("div");
                thumbnail.classList.add("tn-thumbnail");
                if (thumbnails_off) {
                    thumbnail.classList.add("tn-off");
                }
                var img = document.createElement("img");
                img.id = "tn_"+movie.id;
                getThumbnail(e.target.value, movie, function(result){
                    console.log("Got tn for: ",result);
                    console.log("tn: ",result.tn);
                    var img = document.getElementById("tn_"+result.id);
                    img.src = result.tn;
                });
                thumbnail.appendChild(img);
                div.appendChild(thumbnail);
                var title = document.createElement("div");
                title.classList.add("tn-main");
                // Seems our dataset has weird leading double quotes - as per DF cleanup before display
                //title.textContent = movie.title.replace(/^\"/, "");
                var text = document.createTextNode(movie.title.replace(/^\"/, ""));
                title.appendChild(text);
                title.appendChild(createRatingWidget(movie));
                div.appendChild(title);
                //div.appendChild(createRatingWidget(movie));
			});
		} else {
			var div = document.createElement("div");
			div.textContent = results.message;
			movieSearchResults.appendChild(div);
		}
		
	});
}

function resetSearchResults(parentId, childId) {
    var parent = document.getElementById(parentId);
    var oldResults = document.getElementById(childId);
    var newResults = document.createElement("div");
    newResults.id = childId;
    parent.replaceChild(newResults, oldResults);
}

function createRatingWidget(movie){
	var div = document.createElement("div");
	div.id = "ratingWidget" + movie.id;
	div.classList.add('acidjs-rating-stars');
	var form = document.createElement("form");
	form.id = "starForm" + movie.id;
	form.setAttribute("mr-id", movie.id);
	form.onclick = function(e) {
		if (e.target.tagName == 'INPUT') {
			var form = e.target.form;
			form.parentNode.classList.remove('predicted');
			var movieID = form.getAttribute('mr-id');
			e.target.setAttribute('checked', true);
			restService("post", "movie_recommender/rest/rateMovie?id="+movieID+"&rating="+e.target.value, function(result){
                // Don't care about response here - will get top25 on websocket
                //console.log("result from rateMovie post: ",result);
			});
		}
	};
	div.appendChild(form);
	for (var i = 0; i < 10; i++) {
		var groupId = "group-"+movie.id;
		var input = document.createElement("input");
		input.id = groupId+"-"+i
		input.type = 'radio';
		input.name = 'groupId';
		if (movie.rating && (parseInt(movie.rating) == (10 - i))) {
			input.setAttribute('checked', true);
			div.classList.add('predicted');
		}
		input.value = 10 - i;
		form.appendChild(input);
		var label = document.createElement("label");
		label.setAttribute('for', groupId+"-"+i);
		form.appendChild(label);
	}
    return div;
 
}

function createTop25List(top25, original25) {
    top25 = top25 && top25.length ? top25 : [];
    var top25Div = document.getElementById("top25");
    // replace any previous results with new ones
    top25Div.innerHTML = original25 ? 
        "May we also suggest these movies for you: " : 
        "Based on your last rating you may also be interested in these movies: ";
    var top25DivResults = document.createElement("div");
    top25DivResults.id = "top25Results";
    top25Div.appendChild(top25DivResults);
    top25.forEach(function(movie, i){
        if (i < 10) {
            var div = document.createElement("div");
            div.id = "mov_"+movie.id;
            // Seems our dataset has weird leading double quotes - as per DF cleanup before display 
            var text = document.createTextNode(movie.title.replace(/^\"/, ""));
            div.appendChild(text);
            top25DivResults.appendChild(div);
            var ratingWidget = createRatingWidget(movie);
            div.appendChild(ratingWidget);
        } else {
            return;
        }
    });
}

function getThumbnail(originalSearch, movie, cb) {
    console.log("Get tn for: ",movie);
    var title = movie.title.replace(/^\"/, "");
    // Fixme: Year will be in the form (1977) so we have to take off (). Maybe a better regex?
    var tmp = title.match(/\s\((\d{4})\)/g);
    // We really need the year for this to work well.  Not all movies have the year unfortunately (or we need the imdbID)
    var year = tmp && tmp.length > 0 ? tmp[0].match(/(\d{4})/g) : "";
    restService('get', 'http:\/\/www.omdbapi.com\/?t='+originalSearch+'&y='+year+'&plot=short&r=json', function(results){
        console.log(results.Poster || "");
        // Need a default placeholder to return if no thumbnail found.
        cb({id: movie.id, tn: results.Poster || ""});
    });
};

