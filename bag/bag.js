var fs = require('fs');
var pg = require('pg');
var path = require('path');
var QueryStream = require('pg-query-stream');
var Cursor = require('pg-cursor')
var async = require('async');
var ndjson = require('ndjson');
var es = require('event-stream');
var _ = require('highland');
var ndjsonStream = require('../ndjson-stream');
var queries = require('./queries');

// adres > openbareruimte
// openbareruimte > woonplaats
// adres > pand

function runAllQueries(client) {
  // returns a stream of NDJSON data
  var bagStream = function(query) {

    // stream of SQL query results
    var fn = path.join(__dirname, query.name + '.sql');
    var sql = fs.readFileSync(fn, 'utf8');
    var queryStream = client.query(new QueryStream(sql));
    return _(queryStream)
      .map(query.rowToPitsAndRelations);
  };

  var s = _(queries)
    .map(bagStream)
    .merge()
    .flatten();

  return s;
};

// filter out events of a certain type and stream as NDJSON to file
function dest(type) {
  var conf = {
    source: 'bag',
    truncate: true
  };

  return _.pipeline(
    _.where({type: type}),
    ndjsonStream(type, conf)
  );
};

exports.convert = function(config, callback) {

  pg.connect(config.db, function(err, client, done) {

    // fail on error
    if (err)
      return callback(err);

    // all good, start processing all queries
    var s = runAllQueries(client);

    var conf = {
      source: 'bag',
      truncate: true
    };

    function dest(type) {
      return _.pipeline(
        _.where({type: type}),
        _.map(function(obj){
          return JSON.stringify(obj) + '\n';
        })
      );
    }

    // s.fork().pipe(dest('pits')).pipe(process.stdout);
    // s.fork().pipe(dest('relations')).pipe(process.stdout);

    var file_opts = {
      encoding: 'utf8',
      highWaterMark: Math.pow(2, 20)
    }

    s.fork()
      .pipe(dest('pits'))
      .pipe(fs.createWriteStream(path.join(conf.source, conf.source + '.' + 'pits' + '.ndjson'), file_opts));

    // s.fork().pipe(dest('relations')).pipe(fs.createWriteStream(path.join(conf.source, conf.source + '.' + 'relations' + '.ndjson'), 'utf8'));

    // disconnect from PostgreSQL and call callback when done
    s.fork().done(function(){
      client.end();
      callback();
    })

  });
}

// exports.convert = function(config, callback) {
//   pitsAndRelations = require('../pits-and-relations')({
//     source: 'bag',
//     truncate: true
//   });
//
//   pg.connect(config.db, function(err, client, done) {
//     if (err) {
//       callback(err)
//     } else {
//       executeQueries(client, function(err) {
//         client.end();
//         callback(err);
//       });
//     }
//   });
// };


// function executeQueries(client, callback) {
//   async.eachSeries(queries, function(query, callback) {
//     fs.readFile(, 'utf8', function (err, sql) {
//       if (err) {
//         callback(err);
//       } else {
//
//         var queryStream = new QueryStream(sql)
//         client.query(queryStream)
//           .on('end', function() {
//             callback();
//           })
//           .pipe(es.map(function (row, callback) {
//             var emit = query.rowToPitsAndRelations(row);
//
//             async.eachSeries(emit, function(item, callback) {
//               pitsAndRelations.emit(item.type, item.obj, function(err) {
//                 callback(err);
//               });
//             },
//
//             function(err) {
//               callback(err);
//             });
//
//           }));
//       }
//     });
//   }, function(err) {
//     callback(err);
//   });
// }
