var request = require('request');
var yaml = require('js-yaml');
var fs = require('fs');
var joola = require('joola.sdk');

try {
  var config = yaml.safeLoad(fs.readFileSync('./config/default.yml', 'utf8'));
} catch (e) {
  console.log(e);
}

joola.init({host: config.joola.host, APIToken: config.joola.apitoken}, function (err) {
  if (err) throw err;
});

exports.pushDocument = function (doc, collection, callback) {
  joola.insert(collection, doc, function (err, data) {
    if (err) throw err;
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