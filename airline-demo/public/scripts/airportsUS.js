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

var airportsUSFeatureCollection = null;

function AirportsUS() {
    if (airportsUSFeatureCollection === null) {
        airportsUSFeatureCollection = this.airportsUSFeatureCollection = new AirportsUSFeatureCollection();
    }
    return airportsUSFeatureCollection;
}

function AirportsUSFeatureCollection() {
    this.airports = {
        "type": "FeatureCollection",
        "features": []
    };
}

AirportsUSFeatureCollection.prototype.load = function(callback) {
    if (this.airports.features.length === 0) {
        var me = this;
        //console.log("LOADING US AIRPORTS!");
        var rawdata = d3.csv('/data/airports_world.dat', function(d) {
            // Filter out anything that is not in USA or is a train station.
            // Note: Would be good place for spark filter call.
            if (d.country !== 'United States' || d.name.indexOf('Station') !== -1 || 
                d.name.indexOf('Train') !== -1 || d.name.indexOf('Amtrak') !== -1 || 
                d.airportCode === '')
                return null;
            else
                return {
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ parseFloat(d.longitude), parseFloat(d.latitude) ]
                    },
                    "type": "Feature",
                    "properties": {
                        "popupContent": "<p>" + d.name 
                            + " <a href='http://airportcod.es/#airport/" 
                            + d.airportCode.toLowerCase() 
                            + "'>" + d.airportCode+"</a></p>" 
                            + "<div class='polar-chart' id="+d.airportCode+"_chart></div>"
                            + "<div class='polar-chart-options' id="+d.airportCode+"_options></div>"
                            + "<nav class='nav-fillpath'>"
                            + "<a id='"+d.airportCode+"_showopts' class='next' href='javascript:;'>"
                            + "<span class='icon-wrap'> </span>"
                            + "</a>"
                            + "<a id='"+d.airportCode+"_hideopts' class='prev' href='javascript:;'>"
                            + "<span class='icon-wrap'> </span>"
                            + "</a>"
                            + "</nav>",
                        "code": d.airportCode
                    },
                    "id": d.airportID
                };
        }, function(error, rows) {
            //console.log(rows);
            me.airports.features = rows;
            callback(me.airports);
        });
    } else {
        //console.log('AIRPORTS ALREADY LOADED!');
        callback(this.airports);
    }
}

AirportsUSFeatureCollection.prototype.getAirportFeature = function(airportCode, callback) {
    aiportCode = airportCode || 'ABC';
    this.load(function(airports) {
        airports = airports.features || [];
        var match = airports.filter(function(airport){
            return airport.properties.code === airportCode;
        });
        callback(match[0] || {});
    });
}

AirportsUSFeatureCollection.prototype.getAirportCoordinates = function(airportCode, callback) {
    aiportCode = airportCode || 'ABC';
    this.load(function(airports) {
        airports = airports.features || [];
        var match = airports.filter(function(airport){
            return airport.properties.code === airportCode;
        });
        var coords = match.length > 0 ? match[0].geometry.coordinates : [0, 0];
        //console.log('coords: ',coords);
        callback({longitude: coords[0], latitude: coords[1]});
    });
}
