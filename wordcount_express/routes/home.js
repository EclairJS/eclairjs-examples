/**
 * home.js - Default route for GET requests to home page. 
 */

exports.index = function(req, res){
  res.render("index", {title: "Using EclaisJS to Count Words in a File"});
};
