var fs = require('fs');
var stream = require('stream');
var request = require('request');
var csv = require('csv');
var async = require('async');
var ndjson = require('ndjson');
var AdmZip = require('adm-zip');

// GeoNames configuration
var baseUrl = 'http://download.geonames.org/export/dump/';
var columns = [
  'geonameid',
  'name',
  'asciiname',
  'alternatenames',
  'latitude',
  'longitude',
  'featureClass',
  'featureCode',
  'countryCode',
  'cc2',
  'admin1Code',
  'admin2Code',
  'admin3Code',
  'admin4Code',
  'population',
  'elevation',
  'dem',
  'timezone',
  'modificationDate'
];

// Filenames
var pitsFilename = __dirname + '/geonames.pits.ndjson';

//var relationsFilename = __dirname + '/geonames.relations.ndjson';

exports.download = function(config, callback) {
  async.eachSeries(config.countries, function(country, callback) {
    var url = baseUrl + country + '.zip';
    var filename = __dirname + '/' + country + '.zip';

    request
      .get(url)
      .pipe(fs.createWriteStream(filename))
      .on('error', function(err) {
        callback(err);
      })
      .on('finish', function() {
        callback();
      });
  },

  function(err) {
    callback(err);
  });
};

exports.convert = function(config, callback) {
  var pitsStream = fs.createWriteStream(pitsFilename);

  async.eachSeries(config.countries, function(country, callback) {
    var filename = __dirname + '/' + country + '.zip';
    var zip = new AdmZip(filename);

    zip.getEntries().forEach(function(zipEntry) {
      if (zipEntry.entryName == country + '.txt') {

        var bufferStream = new stream.PassThrough();
        bufferStream.end(zipEntry.getData());
        bufferStream
          .pipe(csv.parse({
            delimiter: '\t',
            quote: '\0',
            columns: columns
          }))
          .pipe(csv.transform(function(row) {
            return {
              id: row.geonameid,
              name: row.name,
              type: 'hg:Place',
              geometry: {
                type: 'Point',
                coordinates: [
                  parseFloat(row.longitude),
                  parseFloat(row.latitude)
                ]
              },
              data: {
                featureClass: row.featureClass,
                featureCode: row.featureCode,
                countryCode: row.countryCode,
                cc2: row.cc2,
                admin1Code: row.admin1Code,
                admin2Code: row.admin2Code,
                admin3Code: row.admin3Code,
                admin4Code: row.admin4Code
              },
              uri: 'http://www.geonames.org/' + row.geonameid
            };

          }))
          .pipe(ndjson.serialize())
          .on('data', function(line) {
            pitsStream.write(line, 'utf8');
          })
          .on('error', function(err) {
            callback(err);
          })
          .on('finish', function() {
            callback();
          });
      }
    });
  },

  function(err) {
    pitsStream.end();
    callback(err);
  });
};
