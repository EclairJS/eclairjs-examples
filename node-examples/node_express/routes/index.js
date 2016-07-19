
/*
 * GET home page.
 */

exports.index = function(req, res){
  res.render('index', { title: 'EclairJS-node Express Examples' });
};