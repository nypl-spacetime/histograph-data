var fs = require('fs');
var util = require('util');
var pg = require('pg');
var path = require('path');
var Cursor = require('pg-cursor');
var async = require('async');
var queries = require('./queries');

var pitsAndRelations;

var woonplaats = null;

// Set woonplaats to a specific BAG woonplaats (name + code)
// to only process one single woonplaats
// Examples:
// var woonplaats = {
//   name: 'de-rijp',
//   code: '3553'
// };
// var woonplaats = {
//   name: 'leiden',
//   code: '2088'
// };
// var woonplaats = {
//   name: 'bussum',
//   code: '1331'
// };
// var woonplaats = {
//   name: 'utrecht',
//   code: '3295'
// };

function runAllQueries(client, callback) {
  async.eachSeries(queries, function(query, callback) {
    var filename = query.name;
    if (woonplaats) {
      filename += '.woonplaats';
    }

    filename += '.sql';

    var sql = fs.readFileSync(path.join(__dirname, filename), 'utf8');
    if (woonplaats) {
      sql = sql.replace('{woonplaatscode}', woonplaats.code);
    }

    runQuery(client, sql, query.name, query.rowToPitsAndRelations, function(err) {
      callback(err);
    });
  },

  function(err) {
    callback(err);
  });
}

function runQuery(client, sql, name, rowToPitsAndRelations, callback) {
  var cursor = client.query(new Cursor(sql));
  var cursorSize = 500;
  var count = 0;

  var finished = false;
  async.whilst(function() {
    return !finished;
  },

  function(callback) {

    cursor.read(cursorSize, function(err, rows) {
      if (err) {
        callback(err);
      } else {
        if (!rows.length) {
          finished = true;
          callback();
        } else {

          async.eachSeries(rows, function(row, callback) {
            var emit = rowToPitsAndRelations(row);

            async.eachSeries(emit, function(item, callback) {
              pitsAndRelations.emit(item.type, item.obj, function(err) {
                callback(err);
              });
            },

            function(err) {
              callback(err);
            });
          },

          function(err) {
            count += 1;

            // TODO: create logging function in index.js
            // TODO: use logger from index.js
            console.log(util.format('%d: processed %d rows of %s (%d done)...', count, cursorSize, name, cursorSize * count));
            callback(err);
          });
        }
      }
    });
  },

  function(err) {
    callback(err);
  });
}

exports.convert = function(config, callback) {

  pitsAndRelations = require('../pits-and-relations')({
    dataset: 'bag',
    truncate: true
  });

  pg.connect(config.db, function(err, client, done) {
    if (err) {
      callback(err);
    } else {
      runAllQueries(client, function(err) {
        done();
        client.end();
        callback(err);
      });
    }
  });
};
