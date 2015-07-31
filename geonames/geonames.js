var fs = require('fs');
var path = require('path');
// var stream = require('stream');
var request = require('request');
var csv = require('csv');
var async = require('async');
// var es = require('event-stream');
var zipfile = require('zipfile');
var _ = require('highland');
var R = require('ramda');

var pitsAndRelations;

// GeoNames configuration
var baseUrl = 'http://download.geonames.org/export/dump/';
var baseUri = 'http://sws.geonames.org/';
var allCountries = 'allCountries.zip';
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
  PCLI: 'Country',
  ADM1: 'Province',
  ADM2: 'Municipality',
  PPLX: 'Neighbourhood',
  PPL: 'Place',
  CNL: 'Water'
};

function downloadGeoNamesFile(filename, callback) {
  request
    .get(baseUrl + filename)
    .pipe(fs.createWriteStream(path.join(__dirname, filename)))
    .on('error', function(err) {
      callback(err);
    })
    .on('finish', function() {
      callback();
    });
}

exports.download = function(config, callback) {
  async.eachSeries([allCountries].concat(adminCodesFilenames),
    downloadGeoNamesFile,
    function(err) {

      // Unzip allCountries
      var zf = new zipfile.ZipFile(path.join(__dirname, allCountries));
      var entryName = allCountries.replace('zip', 'txt');
      zf.copyFile(entryName, path.join(__dirname, entryName), function(err) {
        callback(err);
      });
    }
  );
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
          from: baseUri + obj.geonameid,
          to: baseUri + 2750405,
          type: 'liesIn'
        }
      ];
    } else if (obj.featureCode === 'ADM2' && obj.admin1Code) {
      // Municipality
      relations = [
        {
          from: baseUri + obj.geonameid,
          to: baseUri + adminCodes.admin1[obj.countryCode + '.' + obj.admin1Code].geonameid,
          type: 'liesIn'
        }
      ];
    } else if (obj.featureCode.indexOf('PPL') === 0 && obj.admin1Code && obj.admin2Code) {
      var parentObj = adminCodes.admin2[obj.countryCode + '.' + obj.admin1Code + '.' + obj.admin2Code];

      // Place
      if (parentObj && parentObj.geonameid) {
        relations = [
          {
            from: baseUri + obj.geonameid,
            to: baseUri + parentObj.geonameid,
            type: 'liesIn'
          }
        ];
      } else {
        relations = [];
      }
    }
  }

  // else if (obj.countryCode === 'BE') {
  //   // TODO: Belgian hierarchy!
  // }
  return relations;
}

function process(row, adminCodes, callback) {
  var type;
  var featureCode = row.featureCode;

  while (featureCode.length > 0 && !type) {
    type = types[featureCode];
    featureCode = featureCode.slice(0, -1);
  }

  if (type) {
    var emit = [];

    var pit = {
      uri: baseUri + row.geonameid,
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
      }
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
      var filename = path.join(__dirname, 'allCountries.txt');

      var extraUris = {};
      (config.extraUris ? require(config.extraUris) : []).forEach(function(uri) {
        var id = uri.replace('http://sws.geonames.org/', '');
        extraUris[id] = true;
      });

      _(fs.createReadStream(filename, {encoding: 'utf8'}))
        .split()
        .map(R.split('\t'))
        .map(R.zipObj(columns))
        .filter(function(row) {
          return R.contains(row.countryCode, config.countries) || extraUris[row.geonameid];
        })
        .map(function(row) {
          return _.curry(process, row, adminCodes);
        })
        .nfcall([])
        .series()
        .errors(function() {
          callback;
        })
        .done(function() {
          callback;
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
