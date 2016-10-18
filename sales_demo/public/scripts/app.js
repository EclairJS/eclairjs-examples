var parseDate = d3.time.format("%Y%m%d").parse;

var duration = 750;
var maxGraphValue = 300;

function SalesApp(map, storeList) {
  this.map = map;
  this.storeList = storeList;
  this.stores = null;
  this.endDate = new Date("July 01, 2013"); // be very careful of dateStr format!

  var port = location.port ? location.port : '80';

  //this.socket = new WebSocket("ws://localhost:5001");
  var socket = this.socket = new WebSocket("ws://"+location.hostname+":"+port);

  socket.onopen = function() {
    // heart beat, stops bluemix from closing connections
    window.setInterval(function() {
      socket.send(JSON.stringify({heartbeat: true}));
    }, 60000);
  };

  socket.onmessage = function(e) {
    if (e.data) {
      var data = JSON.parse(e.data);
      if (data.type == 'hourlydata') {
        //console.log("Got hourlydata data.data: ",data.data);

        // Capture end date for line graph initialization
        this.endDate = new Date(data.data[0].time);
        //console.log("Just set this.endDate: ",this.endDate);

        this.updateBarGraph(data.data);
      } else if (data.type == 'predictdata') {
        console.log("Got predictdata data.data: ",data.data);
        this.updateLineGraph(data.data);
      }
    }
  }.bind(this);

  this.d3Container = null;
}

SalesApp.prototype.setStores = function(stores) {
  this.stores = stores;

  var scope = this;

  this.stores.forEach(function(store) {
    var marker = L.marker([store.lat, store.lng]).addTo(scope.map);

    marker.bindPopup(L.popup({maxWidth: 1000}));

    marker.on("popupopen", scope.onPopupOpen.bind(scope, store));
    marker.on("popupclose", scope.onPopupClose.bind(scope));
  });
};

SalesApp.prototype.buildBarGraph = function(container) {
  // Data to initialize bar graph with.
  var data = [];
  for (var i = 0; i < 24; i++) {
    data.push({sales: 0, time: new Date(this.endDate).setHours(i)});
  }

  var storeHrs = this.store.hours;
  // Store hours are ordered in array as Mon-Sun but Date.getDay() goes Sun-Mon so have to adjust.
  // Also store hours are in the form "Mon: 10am-9pm".
  var dayOfWeek = this.endDate.getDay();

  var todaysHrs = dayOfWeek > 0 ? storeHrs[dayOfWeek-1] : storeHrs[storeHrs.length-1];
  todaysHrs = todaysHrs.split(": ")[1].split("-");
  //console.log("todaysHrs: ",todaysHrs);

  // Hours can come in as either foramt.
  var timeFormat1 = d3.time.format("%Y-%m-%d %I%p"),
    timeFormat2 = d3.time.format("%Y-%m-%d %H:%M%p");
  var isoDate = this.endDate.toISOString().split("T")[0];
  var open = timeFormat1.parse(isoDate+" "+todaysHrs[0]) || timeFormat2.parse(isoDate+" "+todaysHrs[0]),
    close = timeFormat1.parse(isoDate+" "+todaysHrs[1]) || timeFormat2.parse(isoDate+" "+todaysHrs[1]);

  //console.log("open: ",open);
  //console.log("close: ",close);

  var margin = {top: 20, right: 50, bottom: 30, left: 50},
    width = 500 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

  var xBar = d3.scale.linear()
    .range([0, width]);

  var yBar = this.yBar = d3.scale.linear()
    .range([height, 0]);

  var color = d3.scale.category10();

  var formatTime = d3.time.format("%I%p");
  var formatMinutes = function(d) {
    var date = new Date();
    date.setHours(d);
    return formatTime(date);
  };

  var xAxisBar = d3.svg.axis()
    .scale(xBar)
    .orient("bottom")
    .tickFormat(formatMinutes);

  var yAxisBar = this.barYAxis = d3.svg.axis()
    .scale(yBar)
    .orient("left")
    .ticks(10);

  var svgBar = this.svgBar = container.append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  xBar.domain([open.getHours(), close.getHours()]);
  yBar.domain([0, maxGraphValue]);

  var gAxisBar = svgBar.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(12," + height + ")")
    .call(xAxisBar);

  var xRange = xBar.range(),
    xTicks = xBar.ticks();

  var bar = svgBar.selectAll(".bar")
    .data(data, function(d) {return new Date(d.time).getHours()});

  // add new data
  bar.enter().append("rect")
    .attr("class", "bar")
    .attr("x", function(d) {
      var xDate = new Date(d.time);
      return (xDate.getHours() >= xTicks[0] && xDate.getHours() <= xTicks[xTicks.length-1]) ? xBar(xDate.getHours()) : -1000;
    })
    .attr("y", function(d) { return yBar(d.sales); })
    .attr("height", function(d) { return height - yBar(d.sales); })
    .attr("width", (xRange[1]-xRange[0])/xTicks.length);

  // remove old data
  bar.exit().remove();

  // update data
  bar.attr("y", function(d) { return yBar(d.sales); })
    .attr("height", function(d) { return height - yBar(d.sales); });

  // y axis - add last so that the label is always visible
  svgBar.append("g")
    .attr("class", "y axis")
    .call(yAxisBar)
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 6)
    .attr("dy", ".71em")
    .style("text-anchor", "end")
    .text("Hourly Sales")
    .attr("class", "y title");

  // finally draw the predicted line - yPos will be updated once we get it
  var yPos = yBar(0);
  svgBar.append("line")
    .style("stroke", "steelblue")
    //.style("stroke-dasharray", ("3, 3"))
    .attr("class", "prediction")
    .attr("x1", 0)
    .attr("y1", yPos)
    .attr("x2", width+32)
    .attr("y2", yPos)
};

function getDate(d) {
  var x = new Date(d);
  return new Date(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate())
}

SalesApp.prototype.updateBarGraph = function(newdata) {
  if (!this.svgBar || !newdata) {
    return;
  }

  var storeId = this.store.storeId;
  var duration = 750;
  var yBar = this.yBar;

  // Update the y-axis date - the date change comes at the end of new data 
  // (before the predictedSales which is in last slot).
  var last = newdata && newdata.length ? new Date(newdata[newdata.length-2].time) : null;
  var formatDate = d3.time.format(" %b %e");
  var dateLabel = last ? formatDate(last) : "";

  this.svgBar.select(".y.axis text.y.title")
    .text("Hourly Sales" + dateLabel);

  // update yaxis
  var max = 100;
  var graphData = [];
  var predictedSales = newdata[24];

  for(var i=0; i<24; i++) {
    var item = newdata[i];
    max = (max < item.sales) ? item.sales : max;
    graphData[i] = item;
  }

  // if the current bar's y max is bigger than the max value and the difference is between 81 and 199, don't change the max
  if (this.barYMax && (this.barYMax > max) && ((this.barYMax - max) > 80) && ((this.barYMax - max) < 200)) {
    // the current Y max is fine
  } else {
    this.barYMax = max + 100;
  }

  this.yBar.domain([0, this.barYMax]);
  var yAxis = this.svgBar.select(".y.axis");

  yAxis.transition()
    .duration(duration)
    .ease('linear')
    .call(this.barYAxis);

  // Update the predictedSales line y-position now that yAxis has been adjusted
  var totalPredicted4Today = predictedSales && predictedSales.predictedSales ? predictedSales.predictedSales : 0;
  //console.log('predictedSales = ' + totalPredicted4Today);

  var yPos = this.yBar(totalPredicted4Today);

  this.svgBar.select(".prediction")
    .transition()
    .duration(duration)
    .attr("y1", yPos)
    .attr("y2", yPos);

  // Update the bar heights
  this.svgBar.selectAll(".bar")
    .data(graphData, function(d) {return new Date(d.time).getHours()})
    .transition()
    .duration(duration)
    .attr("y", function(d) { return yBar(d.sales); })
    .attr("height", function(d) { var height = yBar.range()[0]; return height - yBar(d.sales); })

};

SalesApp.prototype.onPopupOpen = function(store, e) {
  console.log("open popup");

  if (this.d3Container) {
    console.log("removing all");
    d3.select(this.d3Container).selectAll("div").select("svg").remove();
    d3.select(this.d3Container).selectAll("div").remove();
    this.d3Container.parentNode.removeChild(this.d3Container)
    this.d3Container = null;
  }

  this.store = store;
  var popup = e.popup;

  console.log("store: ", store);

  var container = document.createElement("div");
  this.d3Container = container;

  d3.select(container).attr("class", "main");

  var label = d3.select(container).append("div").attr("class", "store-label");
  // Use storeId as per DF
  label.text("Store #"+store.storeId);
  this.containerL = d3.select(container).append("div").attr("class", "left");
  this.containerR = d3.select(container).append("div").attr("class", "right");

  popup.setContent(container);
  popup.update();

  // build the graphs after DOM added to document
  this.buildLineGraph(this.containerL);
  this.buildBarGraph(this.containerR);

  this.socket.send(JSON.stringify({registerForStore: this.store.storeId}));

  console.log("popup should be opened");

  /*var myThis = this;

  function update(data) {
    myThis.endDate = getDate(data.data.date);
    console.log(data.data)
    myThis.updateLineGraph(data.data);
  }*/

  /*var d = new Date(1385532000000);

   for (var i = 0; i < 7; i++) {
   var item = {"type":"predictdata","data":{"actualSales":Math.random()*1000,"predictedSales":Math.random()*1000,"date":d3.time.day.offset(d, i)}};
   window.setTimeout(update.bind(this, item), 1000*(i+1))
   }*/

//  update({"type":"predictdata","data":{"actualSales":216,"predictedSales":132.4423126250736,"price":998.786296296294,"date":1385532000000}})
  /*
   var data =   {"type":"hourlydata","data":[{"time":1385942400000,"sales":0},{"time":1385946000000,"sales":0},{"time":1385949600000,"sales":0},{"time":1385953200000,"sales":0},{"time":1385956800000,"sales":0},{"time":1385960400000,"sales":0},{"time":1385964000000,"sales":71},{"time":1385967600000,"sales":87},{"time":1385971200000,"sales":87},{"time":1385974800000,"sales":87},{"time":1385978400000,"sales":87},{"time":1385982000000,"sales":87},{"time":1385985600000,"sales":92},{"time":1385989200000,"sales":96},{"time":1385992800000,"sales":98},{"time":1385996400000,"sales":103},{"time":1386000000000,"sales":122},{"time":1386003600000,"sales":140},{"time":1386007200000,"sales":175},{"time":1386010800000,"sales":213},{"time":1386014400000,"sales":263},{"time":1386018000000,"sales":307},{"time":1386021600000,"sales":1309},{"time":1386025200000,"sales":0}]}
   this.endDate = new Date(data.data[0].time);
   this.updateBarGraph(data.data);
   */
};

SalesApp.prototype.onPopupClose = function(e) {
  this.socket.send(JSON.stringify({unregisterForStore: this.store.storeId}));
};

SalesApp.prototype.buildLineGraph = function(container) {
  var margin = {top: 20, right: 70, bottom: 30, left: 50},
    width = 500 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

  var svg = this.lineGraph = container.append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // build the scales
  var xScale = this.lineXScale = d3.time.scale().range([0, width+50]);
  var yScale = this.lineYScale = d3.scale.linear().range([height, 0]);

  // build the line container
  this.lineLine = d3.svg.line()
    .interpolate("linear")
    .x(function(d) {return xScale(d.date);})
    .y(function(d) {return yScale(d.sales);});

  var legend = svg.append("g")
    .attr("class","legend")
    .attr("transform","translate(50,30)")
    .style("font-size","0px")
    .call(d3.legend)

  setTimeout(function() {
    legend.style("font-size","1.2em")
      .attr("data-style-padding",8)
      .call(d3.legend)
  },1000);

  var data = [];

  for (var i = 0; i < 7; i++) {
    data.push({date: d3.time.day.offset(this.endDate, i-7), predictedSales: 0, actualSales: 0});
  }

  this.data = data;

  this.updateLineGraph();
};

var firstUpdate = true;
var lineColor;

SalesApp.prototype.updateLineGraph = function(newDataItem) {
  var margin = {top: 20, right: 70, bottom: 30, left: 50},
    width = 500 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

  if (newDataItem) {
    // If we loop back to beginning of data then reset to start from beginning
    if (this.data && (new Date(newDataItem.date) < new Date(this.data[this.data.length -1].date))) {
        //console.log("newDateItem date: ",new Date(newDataItem.date).toDateString());
        //console.log("lastItem date: ",new Date(this.data[this.data.length -1].date).toDateString());
        console.log("RESETTING XDOMAIN because of loop this.endDate: ",this.endDate);
        firstUpdate = true;
    }

    // Also if a break in the data is encountered (e.g. the newData point is not for
    // one day after the last data point) reset data for this as well.
    // Note:  Could be a little smarter about this case ans look back over array and keep anything a week
    // back from newDataPoint date - right now just reset data.
    if (this.data && (new Date(this.data[this.data.length -1].date) < d3.time.day.offset(new Date(newDataItem.date), -1))) {
        //console.log("lastItem date: ",new Date(this.data[this.data.length -1].date).toDateString());
        //console.log("newDateItem offset-1 date: ",d3.time.day.offset(new Date(newDataItem.date), -1));
        console.log("RESETTING XDOMAIN beacause of break this.endDate: ",this.endDate);
        firstUpdate = true;
    }

    if (firstUpdate) {
      // starter data
      var data = [];
      for (var i = 0; i < 7; i++) {
        data.push({date: d3.time.day.offset(this.endDate, i-7), predictedSales: 0, actualSales: 0});
      }

      this.data = data;

      firstUpdate = false;
    }

    while (this.data.length > 6) {
      this.data.shift();
    }

    this.data.push(newDataItem)
  }

  var data = this.data;

  if (!lineColor) {
    lineColor = d3.scale.category10();
    lineColor.domain(d3.keys(data[0]).filter(function(key) { return key !== "date"; }));
  }

  var stores = lineColor.domain().map(function(name) {
    return {
      name: name,
      values: data.map(function(d) {
        return {date: getDate(d.date), sales: +d[name]};
      })
    };
  });

  // x axis
  this.lineXScale.domain(d3.extent(stores[0].values, function(d) { return d.date}));
  var xAxis = d3.svg.axis().scale(this.lineXScale).orient("bottom").tickFormat(d3.time.format("%b %e")).ticks(d3.time.days, 1);

  if (this.lineGraph.selectAll(".x.axis")[0].length < 1) {
    // no x axis yet
    this.lineGraph.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0, "+height+")")
      .call(xAxis);
  } else {
    // update x axis
    this.lineGraph.selectAll(".x.axis").transition().duration(duration).call(xAxis);
  }

  // calculate yMin/Max values
  var yMin = 0;
  var yMax = d3.max([d3.max(stores[0].values, function(d) { return d.sales}), d3.max(stores[1].values, function(d) { return d.sales})]) + 100;

  this.lineYScale.domain([yMin, yMax]);

  var yAxis = d3.svg.axis().scale(this.lineYScale).orient("left");

  if (this.lineGraph.selectAll(".y.axis")[0].length < 1) {
    // no y axis yet
    this.lineGraph.append("g")
      .attr("class", "y axis")
      .call(yAxis)
      .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Sales");
  } else {
    // update y axis
    this.lineGraph.selectAll(".y.axis").transition().duration(duration).call(yAxis);
  }

  // generate line paths
  var line = this.lineLine;

  if (this.lineGraph.selectAll(".line")[0].length < 1) {
    var lines = this.lineGraph.selectAll(".line").data(stores);

    lines.transition().duration(duration);

    // enter any new data
    lines.enter()
      .append("path")
      .attr("class","line")
      .attr("d", function(d) {
        return line(d.values);
      })
      .attr("data-legend",function(d) { return d.name })
      .style("stroke", function(d) { return lineColor(d.name); });

    // exit
    lines.exit().remove();
  } else {
    this.lineGraph.selectAll(".line").data(stores).transition()
      .attr("d", function(d) {
        return line(d.values);
      })
  }
};
