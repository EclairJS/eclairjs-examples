var ws;

window.onload = function() {
    var port = location.port ? location.port : '80';
    ws = new WebSocket("ws://"+location.hostname+":"+port);

    // When a message is recieved from NodeJS on the websocket display the results as a simple list.
    ws.onmessage = function(e) {
        if (e.data) {
            var list = document.getElementById("topTen");
            var data = JSON.parse(e.data);
            console.log("data: ",data);
            var results = data.results || [];
            var filename = data.filename || "the text";
            results.forEach(function(item){
                var li = document.createElement("li");
                var text = document.createTextNode("The word '"+item.word+"' appears "+item.count+" times in "+filename);
                li.appendChild(text);
                list.appendChild(li);
            });
        }
    };

};

// When the user clicks the button let NodeJS know it can start the EclairJS counting piece.
function clickme() {
    if (ws) {
        // First clear out any old results
        var list = document.getElementById("topTen");
        while(list.hasChildNodes()) {
            list.removeChild(list.children[0]);
        }
        // Get the value of the optional input.
        var optional = document.getElementById("filename");
        var filename = optional && optional.value ? optional.value : "";
        ws.send(JSON.stringify({startCount: true, filename: filename}));
    }
};

