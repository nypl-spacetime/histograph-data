var fs = require('fs');
var pg = require('pg');
var path = require('path');
var QueryStream = require('pg-query-stream');
var Cursor = require('pg-cursor')
var async = require('async');
var es = require('event-stream');

var pitsAndRelations;

var queries = [
  'woonplaats'
];

function executeQueries(client, callback) {
  async.eachSeries(queries, function(query, callback) {
    fs.readFile(path.join(__dirname, query + '.sql'), 'utf8', function (err, sql) {
      if (err) {
        callback(err);
      } else {

        var query = new QueryStream(sql)
        client.query(query)
          .on('end', function() {
            callback();
          })
          .pipe(es.map(function (row, callback) {
            var emit = [];

            var pit = {
              id: row.id,
              name: row.name,
              type: 'hg:Place',
              geometry: JSON.parse(row.geometry),
              data: {
              },
              uri: ''
            };

            emit.push({
              type: 'pits',
              obj: pit
            });

            // emit = emit.concat(getRelations(adminCodes, row).map(function(relation) {
            //   return {
            //     type: 'relations',
            //     obj: relation
            //   };
            // }));

            async.eachSeries(emit, function(item, callback) {
              pitsAndRelations.emit(item.type, item.obj, function(err) {
                callback(err);
              });
            },

            function(err) {
              callback(err);
            });

          }));
      }
    });
  }, function(err) {
    callback(err);
  });
}

exports.convert = function(config, callback) {
  pitsAndRelations = require('../pits-and-relations')({
    source: 'bag',
    truncate: true
  });

  pg.connect(config.db, function(err, client, done) {
    if (err) {
      callback(err)
    } else {
      executeQueries(client, function(err) {
        client.end();
        callback(err);
      });
    }
  });
};



