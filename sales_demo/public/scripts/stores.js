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

function StoreList() {
}

StoreList.prototype.getStoresByCity = function(city, callback) {
    var data = [];
    var storeHrs1 = "Mon: 9am-9pm; Tue: 9am-9pm; Wed: 9am-9pm; Thurs: 9am-9pm; Fri: 9am-10pm; Sat: 9am-10pm; Sun: 10am-9pm";
    var storeHrs2 = "Mon: 10am-9pm; Tue: 10am-9pm; Wed: 10am-9pm; Thurs: 10am-9pm; Fri: 10am-9pm; Sat: 10am-9pm; Sun: 10am-9pm";
    data.push({ "storeId": 960, "name": "BlueStore 960", "lat": 40.7349210, "lng": -73.9804827, "hours": storeHrs1.split("; ")});
    data.push({ "storeId": 105, "name": "BlueStore 105", "lat": 40.754333, "lng": -73.980011, "hours": storeHrs2.split("; ")});
    data.push({ "storeId": 2201, "name": "BlueStore 2201", "lat": 40.769882, "lng": -73.982094, "hours": storeHrs1.split("; ")});
    data.push({ "storeId": 1028, "name": "BlueStore 1028", "lat": 40.76078, "lng": -73.983487, "hours": storeHrs2.split("; ")});
    data.push({ "storeId": 609, "name": "BlueStore 609", "lat": 40.7349109, "lng": -73.9905737, "hours": storeHrs1.split("; ")});
    data.push({ "storeId": 11, "name": "BlueStore 11", "lat": 40.73445, "lng": -73.9899, "hours": storeHrs2.split("; ")});
    data.push({ "storeId": 876, "name": "BlueStore 876", "lat": 40.74194, "lng": -73.99295, "hours": storeHrs1.split("; ")});
    data.push({ "storeId": 1010, "name": "BlueStore 1010", "lat": 40.72551, "lng": -73.996689, "hours": storeHrs2.split("; ")});
    callback(data);
};

