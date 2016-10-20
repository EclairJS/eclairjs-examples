var net = require('net');

function NetStream(port, data) {
  this.server = net.createServer(function(socket) {
    this.interval = setInterval(function() {
      // randomly grap an item from the data array
      var index = Math.floor(Math.random() * (data.length));
      socket.write(data[index] + "\n");
    }, 1000);
  }.bind(this)).listen(port);
}

NetStream.prototype.stop = function() {
  clearInterval(this.interval);

  var p = new Promise(function(resolve, reject) {
    this.server.close(resolve);
  });
};

module.exports = NetStream;