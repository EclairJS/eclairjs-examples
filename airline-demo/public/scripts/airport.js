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

function Airport(options, map, socket) {
    this.airportCode = options.airportCode;
    this.latitude = options.latitude;
    this.longitude = options.longitude;
    this.feature = options.feature;
    this.map = map;
    this.polarChart = new PolarChart(map, this.feature);
    this.socket = socket;

    this.createChart();
};

Airport.prototype.createChart = function() {
    this.polarChart.create(this);
    this.getCarriersForFlightsToday(function(data){
        //console.log('Got carriers for ',this.airportCode,' carriers: ',data);
        this.polarChart.setCarrierOptions(data);
    }.bind(this));
};

Airport.prototype.updateFlights = function(data) {
    var airportCode = this.airportCode || 'ABC';

    // Longitude and Latitude for the origin airport the user selected from the map.
    var long = this.longitude || 0;
    var lat = this.latitude || 0;

    var me = this;

    var bearingFromCoordinates = function(lat1, long1, lat2, long2) {
        var brng = Math.atan2(lat2 - lat1, long2 - long1);
        brng = brng * (180/Math.PI);
        brng = (brng + 360) % 360;
        brng = 360 - brng;
        return brng;
    }

    var getRegionFromBearing = function(bearing) {
        var unadjustedRegion = Math.floor(bearing/30);
        // Note: Our chart re-orients labels so zero degrees is at label N
        // and region 0 is from N-3 degrees, but to get proper angle we have
        // to re-orient along x-axis.
        return unadjustedRegion < 9 ? unadjustedRegion+3 : unadjustedRegion-9;
    }

    var delay_range = this.polarChart.getDelayRange();
    delay_range = (delay_range && delay_range.length > 1) ? delay_range : [10, 60];
    var ONTIME_THRESHOLD = delay_range[0],
        MINOR_DELAY_THRESHOLD = delay_range[1];

    //console.log('Using an ontime threshold of: ', ONTIME_THRESHOLD, ' for: ',airportCode);
    //console.log('Using a minor delay threshold of: ', MINOR_DELAY_THRESHOLD, ' for: ',airportCode);

    var regiondata = [];
    var airportsUS = new AirportsUS();
    var flights = data || [];
    flights = flights.filter(function(flight){ return flight.origin === airportCode });
    for (var i = 0; i < flights.length; i++) {
        airportsUS.getAirportCoordinates(flights[i].destination, function(coords){
            var bearing = bearingFromCoordinates(lat, long, coords.latitude, coords.longitude);
            var region = getRegionFromBearing(bearing);
            var status = flights[i].take_off_delay_mins <= ONTIME_THRESHOLD
                ? 'ontime' : flights[i].take_off_delay_mins <= MINOR_DELAY_THRESHOLD
                ? 'delay_minor' : 'delay_major';
            // Need to check if region/status combo already exists and update totals, otherwise add new one.
            var match = regiondata.filter(function(d){return d.region === region});
            if (match.length === 0) {
                regiondata.push({region: region, ontime: 0, delay_minor: 0, delay_major: 0});
                regiondata[regiondata.length-1][status]++;
            } else {
                match[0][status]++;
            }
        });
    };

    // Note: For each region 0-11, chartdata will be in the form:
    //var chartdata = [
    //    {region:0, ontime:25},
    //    {region:0, delay_minor:50},
    //    {region:0, delay_major:25}
    //  ]

    // Calculate overall percentages for each region.
    var chartdata = [];
    for (var j = 0; j < regiondata.length; j++) {
        var total = regiondata[j].ontime + regiondata[j].delay_minor + regiondata[j].delay_major;
        chartdata.push({region: regiondata[j].region, ontime: Math.round((regiondata[j].ontime/total)*100)});
        chartdata.push({region: regiondata[j].region, delay_minor: Math.round((regiondata[j].delay_minor/total)*100)});
        chartdata.push({region: regiondata[j].region, delay_major: Math.round((regiondata[j].delay_major/total)*100)});
    }

    //console.log('chartdata: ',chartdata);
    this.polarChart.setChartData(chartdata);
}

Airport.prototype.getCarriersForFlightsToday = function(callback) {
    var airportCode = this.airportCode || 'ABC';
    if (this.carriersForToday) {
        callback(this.carriersForToday);
    } else {
        var me = this;
        $.get("/getCarriers?airport="+airportCode, function(data) {
            //console.log('getCarriersForFlightsToday rawdata: ',data);
            this.carriersForToday = data || [];
            callback(this.carriersForToday);
        });
    }
}

Airport.prototype.getScheduleByCarrierForToday = function(callback, carrier) {
    var airportCode = this.airportCode || 'ABC';
    carrier = carrier || 'AA';
        if (this.schedulesForToday && this.schedulesForToday[carrier]) {
        callback(this.schedulesForToday[carrier]);
    } else {
        var me = this;
        $.get("/getSchedule?airport="+airportCode+"&carrier="+carrier, function(data) {
            //console.log('getScheduleByCarrier rawdata: ',data);
            this.schedulesForToday = this.schedulesForToday || [];
            this.schedulesForToday[carrier] = data;
            callback(this.schedulesForToday[carrier]);
        });
    }
}

