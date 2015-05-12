var fs = require('fs');
var pg = require('pg');
var path = require('path');
var QueryStream = require('pg-query-stream');
var Cursor = require('pg-cursor')
var async = require('async');
var es = require('event-stream');

var pitsAndRelations;

// adres > openbareruimte
// openbareruimte > woonplaats
// adres > pand

var queries = [
  {
    name: 'pand',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        type: 'hg:Building',
        data: {
          bouwjaar: parseInt(row.bouwjaar)
        },
        geometry: JSON.parse(row.geometry)
      };

      return [
        {
          type: 'pits',
          obj: pit
        }
      ];
    }
  },

  {
    name: 'openbareruimte',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: row.name,
        type: 'hg:Street',
        data: {
          woonplaatscode: parseInt(row.woonplaatscode)
        }
      };

      var relation = {
        from: parseInt(row.id),
        to: parseInt(row.woonplaatscode),
        label: 'hg:liesIn'
      };

      return [
        {
          type: 'pits',
          obj: pit
        },
        {
          type: 'relations',
          obj: relation
        }
      ];
    }
  },

  {
    name: 'woonplaats',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: row.name,
        type: 'hg:Place',
        geometry: JSON.parse(row.geometry),
        data: {
          gemeentecode: parseInt(row.gemeentecode)
        }
      };

      return [
        {
          type: 'pits',
          obj: pit
        }
      ];
    }
  },

  {
    name: 'verblijfsobject',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: [row.openbareruimtenaam, row.huisnummer, row.huisletter, row.huisnummertoevoeging].filter(function(p) {
            return p;
          }).join(' '),
        type: 'hg:Address',
        geometry: JSON.parse(row.geometry),
        data: {
          openbareruimte: parseInt(row.openbareruimte),
          postcode: row.postcode
        }
      };

      var relation = {
        from: parseInt(row.id),
        to: parseInt(row.openbareruimte),
        label: 'hg:liesIn'
      };

      var result = [
        {
          type: 'pits',
          obj: pit
        },
        {
          type: 'relations',
          obj: relation
        }
      ];

      if (row.pand_ids) {
        row.pand_ids.split(',').forEach(function(pandId) {
          result.push({
            type: 'relations',
            obj: {
              from: parseInt(row.id),
              to: parseInt(pandId),
              label: 'hg:liesIn'
            }
          });
        });
      }

      return result;
    }
  }
];

function executeQueries(client, callback) {
  async.eachSeries(queries, function(query, callback) {
    fs.readFile(path.join(__dirname, query.name + '.sql'), 'utf8', function (err, sql) {
      if (err) {
        callback(err);
      } else {

        var queryStream = new QueryStream(sql)
        client.query(queryStream)
          .on('end', function() {
            callback();
          })
          .pipe(es.map(function (row, callback) {
            var emit = query.rowToPitsAndRelations(row);

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



