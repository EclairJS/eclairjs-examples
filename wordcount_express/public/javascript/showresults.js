window.onload = function() {
    var port = location.port ? location.port : '80';
    ws = new WebSocket("ws://"+location.hostname+":"+port);

    // When a message is recieved from NodeJS on the websocket display the results as a simple list.
    ws.onmessage = function(e) {
        if (e.data) {
            var list = document.getElementById("topTen");
            var data = JSON.parse(e.data);
            //console.log("data: ",data);
            data.forEach(function(item){
                var li = document.createElement("li");
                var text = document.createTextNode("The word '"+item.word+"' appears "+item.count+" times in the text");
                li.appendChild(text);
                list.appendChild(li);
            });
        }
    };

    // When the websocket is open let NodeJS know it can start the EclairJS piece.
    ws.onopen = function() {
        ws.send(JSON.stringify({startCount: true}));
    };
};
