var toshi = require('./lib/toshi');
var joola = require('./lib/joola');
var async = require('async');

var current_height = 0;
var block_counter = 1;
var tx_counter = 1;

function pushTransaction(height) {
  if (height <= current_height) {
    toshi.getBlock(height, function (data) {
      if (data.transaction_hashes) {
        async.map(data.transaction_hashes, function (item, callback) {
          toshi.getTransaction(item, function (t) {
            delete t.inputs;
            delete t.outputs;
            t.timestamp = t.block_time;
            t.transaction_counter = 1;
            t.block_height = data.height;
            t.block_height_str = data.height.toString();
            t.size_str = t.size.toString();
            t.amount_str = t.amount.toString();
            t.fees_str = t.fees.toString();
            t.confirmations_str = t.confirmations.toString();
            t.push_timestamp = new Date().toISOString();
            return callback(null, t);
          });
        }, function (err, results) {
          joola.pushDocument(results, 'transactions', function () {
            console.log('pushed ' + results.length + ' txs for block', data.height);
          });
        });
      }
    });
  }
}

function pushBlock(height) {
  if (height <= current_height) {
    toshi.getBlock(height, function (data) {
      if (!data.error) {
        var date = new Date(data.time);
        data.timestamp = date.toISOString();
        data.block_counter = 1;
        data.height_str = data.height.toString();
        data.fees_str = data.fees.toString();
        data.bits_str = data.bits.toString();
        data.size_str = data.size.toString();
        data.reward_str = data.reward.toString();
        data.total_out_str = data.total_out.toString();
        data.transactions_count_str = data.transactions_count.toString();
        data.push_timestamp = new Date().toISOString();
        delete data.next_blocks;
        delete data.transaction_hashes;
        joola.pushDocument(data, 'blocks', function () {
          console.log('pushed block', data.height);
        });
      }
    });
  }
}

function addBlockTasks(num, interval, joola_height) {
  var tasks = [];
  for (var i = 1; i <= num; i++) {
    tasks.push(function (callback) {
      setTimeout(function () {
        pushBlock(joola_height + block_counter);
        block_counter++;
        callback(null);
      }, interval);
    });
  }
  async.series(tasks, function (err, result) {
    joola_height += num;
    block_counter = 1;
    addBlockTasks(num, interval, joola_height + 1);
  });
}

function addTransactionsTask(num, interval, joola_height) {
  var tasks = [];
  for (var i = 1; i <= num; i++) {
    tasks.push(function (callback) {
      setTimeout(function () {
        pushTransaction(joola_height + tx_counter);
        tx_counter++;
        callback(null);
      }, interval);
    });
  }
  async.series(tasks, function (err, result) {
    joola_height += num;
    tx_counter = 1;
    addTransactionsTask(num, interval, joola_height + 1);
  });
}

toshi.getLastBlock(function (block) {
  current_height = block.height;
  joola.getLastItem('blocks', 'height', function (data) {
    var joola_height = 0;
    if (data[0].documents && data[0].documents.length > 0)
      joola_height = data[0].documents[0].values.height;
    addBlockTasks(100, 100, joola_height + 1);
  });
});

toshi.getLastBlock(function (block) {
  joola.getLastItem('transactions', 'block_height', function (data) {
    var joola_height = 0;
    if (data[0].documents && data[0].documents.length > 0)
      joola_height = data[0].documents[0].values.block_height;
    addTransactionsTask(100, 100, joola_height + 1);
  });
});
 