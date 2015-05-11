var fs = require('fs');
var path = require('path');
var stream = require('stream');
var request = require('request');
var async = require('async');
var es = require('event-stream');
var AdmZip = require('adm-zip');
var shapefile = require('shapefile');

var baseUrl = 'http://geodata.nationaalgeoregister.nl/nwbwegen/extract/';
var filename = 'nwbwegen.zip';
var shapefileName = 'Wegvakken';

var pitsAndRelations;

function extractRoadSegments(config, zipfile, callback) {
  var zip = new AdmZip(zipfile);
  var zipEntries = [];
  zip.getEntries().forEach(function(zipEntry) {
    var parts = zipEntry.entryName.split('/');
    if (parts[parts.length - 1].indexOf(shapefileName) == 0) {
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

exports.download = function(config, callback) {
  request
    .get(baseUrl + filename)
    .pipe(fs.createWriteStream(path.join(__dirname, filename)))
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      extractRoadSegments(config, path.join(__dirname, filename), function(err) {
        callback(err);
      });
    });
};

exports.convert = function(config, callback) {
  pitsAndRelations = require('../pits-and-relations')({
    source: 'nwb',
    truncate: true
  });

  var reader = shapefile.reader(path.join(__dirname, shapefileName + '.shp'));

  reader.readHeader(function(error, header) {
    readAllRecords(function(err) {
      callback(err);
    });
  });

  function readAllRecords(callback) {
    (function readRecord() {
      reader.readRecord(function(error, record) {
        if (error) return callback(error);
        if (record === shapefile.end) return callback(null);

        var emit = [];

        var pit = {
          id: record.properties.WVK_ID,
          name: record.properties.STT_NAAM,
          type: 'hg:Street',
          geometry: record.geometry
        };

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
          process.nextTick(readRecord);
        });

      });
    })();
  }
};

exports.done = function(config, callback) {
  if (pitsAndRelations) {
    pitsAndRelations.close();
  }
  callback();
};
