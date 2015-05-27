var fs = require('fs');
var path = require('path');
var request = require('request');
var async = require('async');
var es = require('event-stream');
var AdmZip = require('adm-zip');
var shapefile = require('shapefile');
var levelup = require('level');
var db = levelup(path.join(__dirname, 'db'), {
  valueEncoding: 'json'
});
var proj4 = require('proj4');
var rd = '+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +towgs84=565.2369,50.0087,465.658,-0.406857330322398,0.350732676542563,-1.8703473836068,4.0812 +no_defs';
var wgs84 = '+title=WGS 84 (long/lat) +proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees';
var urlify = require('urlify').create({
  addEToUmlauts: true,
  szToSs: true,
  toLower: true,
  spaces: '-',
  nonPrintable: '-',
  trim: true
});

var baseUrl = 'http://geodata.nationaalgeoregister.nl/nwbwegen/extract/';
var filename = 'nwbwegen.zip';
var shapefileName = 'Wegvakken';

var pitsAndRelations;

function extractRoadSegments(config, zipfile, callback) {
  var zip = new AdmZip(zipfile);
  var zipEntries = [];
  zip.getEntries().forEach(function(zipEntry) {
    var parts = zipEntry.entryName.split('/');
    if (parts[parts.length - 1].indexOf(shapefileName) === 0) {
      zipEntries.push({
        filename: parts[parts.length - 1],
        zipEntry: zipEntry
      });
    }
  });

  async.eachSeries(zipEntries, function(zipEntry, callback) {
    var stream = fs.createWriteStream(path.join(__dirname, zipEntry.filename), {flags: 'w'});
    stream.write(zipEntry.zipEntry.getData(), function(err) {
      callback(err);
    });
  },

  function(err) {
    callback(err);
  });
}

function storeRecord(record, callback) {
  var dbKey = record.properties.WPSNAAMNEN + '-' + record.properties.STT_NAAM;
  db.get(dbKey, function(err, value) {
    if (err) {
      value = {};
      value[record.properties.WVK_ID] = record;

      db.put(dbKey, value, function(err) {
        callback(err);
      });
    } else {
      value[record.properties.WVK_ID] = record;
      db.put(dbKey, value, function(err) {
        callback(err);
      });
    }
  });
}

function storeRecords(callback) {
  var reader = shapefile.reader(path.join(__dirname, shapefileName + '.shp'));

  reader.readHeader(function() {
    readAllRecords(function(err) {
      callback(err);
    });
  });

  function readAllRecords(callback) {
    (function readRecord() {
      reader.readRecord(function(error, record) {
        if (error) return callback(error);
        if (record === shapefile.end) return callback(null);

        storeRecord(record, function(err) {
          if (err) {
            callback(err);
          } else {
            process.nextTick(readRecord);
          }
        });
      });
    })();
  }
}

exports.download = function(config, callback) {
  request
    .get(baseUrl + filename)
    .pipe(fs.createWriteStream(path.join(__dirname, filename)))
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      extractRoadSegments(config, path.join(__dirname, filename), function(err) {
        if (err) {
          callback(err);
        }

        storeRecords(function(err) {
          callback(err);
        });
      });
    });
};

exports.convert = function(config, callback) {
  pitsAndRelations = require('../pits-and-relations')({
    source: 'nwb',
    truncate: true
  });

  db.createReadStream()
    .pipe(es.map(function(data, callback) {
      var wvkIds = Object.keys(data.value);

      var id = urlify(data.key);
      var name = data.value[wvkIds[0]].properties.STT_NAAM;

      var coordinates = [];
      wvkIds.forEach(function(wvkId) {
        var geometry = data.value[wvkId].geometry;
        var projectedCoordinates = geometry.coordinates.map(function(coordinate) {
          return proj4(rd, wgs84, coordinate);
        });

        coordinates.push(projectedCoordinates);
      });

      var geometry = {
        type: 'MultiLineString',
        coordinates: coordinates
      };

      var pit = {
        id: id,
        name: name,
        type: 'hg:Street',
        data: {
          wvk_ids: wvkIds.map(function(wvkId) { return parseInt(wvkId); })
        },
        geometry: geometry
      };

      var emit = [];

      emit.push({
        type: 'pits',
        obj: pit
      });

      // TODO: add relation with place/municipality
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
    }))
    .on('error', function(err) {
      callback(err);
    })
    .on('close', function() {
      callback();
    })
    .on('end', function() {
      callback();
    });
};

exports.done = function(config, callback) {
  if (pitsAndRelations) {
    pitsAndRelations.close();
  }

  callback();
};
