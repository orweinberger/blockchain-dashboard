var request = require('request');
var yaml = require('js-yaml');
var fs = require('fs');

try {
  var config = yaml.safeLoad(fs.readFileSync('./config/default.yml', 'utf8'));
} catch (e) {
  console.log(e);
}

exports.pushDocument = function (doc, collection, callback) {
  var options = {
    uri: config.joola.host + "/beacon/demo/" + collection + "?APIToken=" + config.joola.apitoken,
    method: "POST",
    json: doc
  };
  request(options, function (err, body, data) {
    if (body.statusCode != 200)
      throw data;
    return callback();
  });
};

exports.getLastItem = function (collection, metric, callback) {
  var options = {
    uri: config.joola.host + "/query/?APIToken=" + config.joola.apitoken,
    method: "POST",
    json: [
      { timeframe: 'last_1_items', metrics: [metric], dimensions: ['timestamp', 'hash'], collection: collection}
    ]
  };
  request(options, function (err, body, data) {
    if (body.statusCode != 200)
      throw data;
    return callback(data);
  });
}