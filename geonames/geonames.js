var fs = require('fs');
var path = require('path');
var stream = require('stream');
var request = require('request');
var csv = require('csv');
var async = require('async');
var es = require('event-stream');
var AdmZip = require('adm-zip');

var pitsAndRelations;

// GeoNames configuration
var baseUrl = 'http://download.geonames.org/export/dump/';
var baseUri = 'http://sws.geonames.org/';
var adminCodesFilenames = [
  'admin2Codes.txt',
  'admin1CodesASCII.txt'
];
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
var types = {
  PCLI: 'hg:Country',
  ADM1: 'hg:Province',
  ADM2: 'hg:Municipality',
  PPL: 'hg:Place',
  CNL: 'hg:Water'
};

exports.download = function(config, callback) {
  var countryFilenames = config.countries.map(function(country) {
    return country + '.zip';
  });

  async.eachSeries(countryFilenames.concat(adminCodesFilenames), function(filename, callback) {
    request
      .get(baseUrl + filename)
      .pipe(fs.createWriteStream(path.join(__dirname, filename)))
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

function getAdminCodes(config, callback) {
  var adminCodes = {
    admin1: {},
    admin2: {}
  };

  async.eachSeries(adminCodesFilenames, function(adminCodesFilename, callback) {

    fs.createReadStream(path.join(__dirname, adminCodesFilename), {
      encoding: 'utf8'
    })
    .pipe(csv.parse({
      delimiter: '\t',
      quote: '\0',
      columns: ['code', 'name', 'asciiname', 'geonameid']
    }))
    .on('data', function(obj) {
      if (config.countries.indexOf(obj.code.substring(0, 2)) > -1) {
        var adminLevel = adminCodesFilename.replace('CodesASCII.txt', '').replace('Codes.txt', '');
        adminCodes[adminLevel][obj.code] = obj;
      }
    })
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      callback();
    });
  },

  function(err) {
    callback(err, adminCodes);
  });
}

function getRelations(adminCodes, obj) {
  var relations = [];
  if (obj.countryCode === 'NL') {
    if (obj.featureCode === 'ADM1') {
      // Province
      relations = [
        {
          from: obj.geonameid,
          to: 2750405,
          label: 'hg:liesIn'
        }
      ];
    } else if (obj.featureCode === 'ADM2' && obj.admin1Code) {
      // Municipality
      relations = [
        {
          from: obj.geonameid,
          to: adminCodes.admin1[obj.countryCode + '.' + obj.admin1Code].geonameid,
          label: 'hg:liesIn'
        }
      ];
    } else if (obj.featureCode === 'PPL' && obj.admin1Code && obj.admin2Code) {
      // Place
      relations = [
        {
          from: obj.geonameid,
          to: adminCodes.admin2[obj.countryCode + '.' + obj.admin1Code + '.' + obj.admin2Code].geonameid,
          label: 'hg:liesIn'
        }
      ];
    }
  }

  // else if (obj.countryCode === 'BE') {
  //   // TODO: Belgian hierarchy!
  // }
  return relations;
}

exports.convert = function(config, callback) {
  pitsAndRelations = require('../pits-and-relations')({
    source: 'geonames',
    truncate: true
  });

  getAdminCodes(config, function(err, adminCodes) {
    if (err) {
      callback(err);
    } else {
      async.eachSeries(config.countries, function(country, callback) {
        var filename = path.join(__dirname, country + '.zip');
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
              .pipe(es.map(function(row, callback) {
                var type = types[row.featureCode];
                if (type) {
                  var emit = [];

                  var pit = {
                    id: row.geonameid,
                    name: row.name,
                    type: type,
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
                    uri: baseUri + row.geonameid
                  };

                  emit.push({
                    type: 'pits',
                    obj: pit
                  });

                  emit = emit.concat(getRelations(adminCodes, row).map(function(relation) {
                    return {
                      type: 'relations',
                      obj: relation
                    };
                  }));

                  async.eachSeries(emit, function(item, callback) {
                    pitsAndRelations.emit(item.type, item.obj, function(err) {
                      callback(err);
                    });
                  },

                  function(err) {
                    callback(err);
                  });
                } else {
                  callback();
                }
              }))
              .on('error', function(err) {
                callback(err);
              })
              .on('end', function() {
                callback();
              });
          }
        });
      },

      function(err) {
        callback(err);
      });
    }
  });
};

exports.done = function(config, callback) {
  if (pitsAndRelations) {
    pitsAndRelations.close();
  }
  callback();
};
