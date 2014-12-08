var request = require('request');
exports.getBlock = function(height, callback) {
  var options = {
    uri: "https://bitcoin.toshi.io/api/v0/blocks/" + height,
    method: "GET"
  };
  request(options, function (err, headers, data) {
    return callback(JSON.parse(data));
  });
};

exports.getTransaction = function(hash, callback) {
  var options = {
    uri: "https://bitcoin.toshi.io/api/v0/transactions/" + hash,
    method: "GET"
  };
  request(options, function (err, headers, data) {
    return callback(JSON.parse(data));
  });
};

exports.getLastBlock = function(callback) {
  var options = {
    uri: "https://bitcoin.toshi.io/api/v0/blocks/latest",
    method: "GET"
  };
  request(options, function (err, headers, data) {
    return callback(JSON.parse(data));
  });
}