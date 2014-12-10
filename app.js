var toshi = require('./lib/toshi');
var joola = require('./lib/joola');
var async = require('async');

var current_height = 0;
var block_counter = 0;
var tx_counter = 0;
/*
function pushTransaction(height) {
  if (height <= current_height) {
    toshi.getBlock(height, function (data) {
      if (data.transaction_hashes) {
        async.map(data.transaction_hashes, function (item, callback) {
          toshi.getTransaction(item, function (t) {
            delete t.inputs;
            delete t.outputs;
            t.timestamp = new Date(t.block_time).toISOString();
            t.transaction_counter = 1;
            t.block_height = data.height;
            t.block_height_str = data.height.toString();
            t.size_str = t.size.toString();
            t.amount_str = t.amount.toString();
            t.fees_str = t.fees.toString();
            t.confirmations_str = t.confirmations.toString();
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
*/
function pushTransaction(height) {
  if (height != 0) {
    toshi.getBlockTransactions(height, 1000, 0, function (data) {
      var count = Math.ceil(data.transactions_count / 1000);
      if (count === 1) {
        async.map(data.transactions, function (t, callback) {
          delete t.inputs;
          delete t.outputs;
          t.timestamp = new Date(t.block_time).toISOString();
          t.transaction_counter = 1;
          t.block_height = data.height;
          t.block_height_str = data.height.toString();
          t.size_str = t.size.toString();
          t.amount_str = t.amount.toString();
          t.fees_str = t.fees.toString();
          t.confirmations_str = t.confirmations.toString();
          return callback(null, t);
        }, function (err, results) {
          joola.pushDocument(results, 'transactions', function () {
            console.log('pushed ' + results.length + ' txs for block', data.height);
          });
        });
      }
      else {
        for (var i = 1; i <= count; i++) {
          toshi.getBlockTransactions(height, 1000, 1000 * i - 1000, function (data) {
            async.map(data.transactions, function (t, callback) {
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
            }, function (err, results) {
              joola.pushDocument(results, 'transactions', function () {
                console.log('pushed ' + results.length + ' txs for block', data.height);
              });
            });
          });
        }
      }
    });
  }
}

function pushBlock(height) {
  if (height <= current_height) {
    toshi.getBlock(height, function (data) {
      if (!data.error) {
        data.timestamp = new Date(data.time).toISOString();
        data.block_counter = 1;
        data.height_str = data.height.toString();
        data.fees_str = data.fees.toString();
        data.bits_str = data.bits.toString();
        data.size_str = data.size.toString();
        data.reward_str = data.reward.toString();
        data.total_out_str = data.total_out.toString();
        data.transactions_count_str = data.transactions_count.toString();
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
    block_counter = 0;
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
    tx_counter = 0;
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
 