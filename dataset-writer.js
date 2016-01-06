var fs = require('fs');
var path = require('path');
var schemas = require('histograph-schemas');
var H = require('highland');
var geojsonhint = require('geojsonhint');
var validator = require('is-my-json-valid');

var validators = {
  pits: validator(schemas.pits),
  relations: validator(schemas.relations)
};

module.exports = function(dataset, dir) {

  function createWriteStream(type) {
    return fs.createWriteStream(path.join(dir, dataset + '.' + type + '.ndjson'), {
      flags: 'w',
      encoding: 'utf8'
    });
  }

  var streams = {};

  this.writeObject = function(data, callback) {
    var type = data.type;
    var obj = data.obj;

    // pit => pits, relation => relations
    var pluralType = type + 's';

    var jsonValid = validators[pluralType](obj);
    var valid = true;
    var errors;

    if (!jsonValid) {
      errors = {
        errors: validators[pluralType].errors,
        data: data.obj
      };
      valid = false;
    } else if (type === 'pit' && obj.geometry) {
      var geojsonErrors = geojsonhint.hint(obj.geometry);
      if (geojsonErrors.length > 0) {
        errors = {
          errors: geojsonErrors,
          data: data.obj
        };
        valid = false;
      }
    }

    if (!valid) {
      setImmediate(callback, errors);
    } else {
      if (!streams[pluralType]) {
        streams[pluralType] = createWriteStream(pluralType);
      }

      streams[pluralType].write(JSON.stringify(obj) + '\n', function() {
        callback();
      });
    }
  };

  this.writeObjects = function(data, callback) {
    var error = false;

    H(data)
      .map(H.curry(this.writeObject))
      .flatten()
      .nfcall([])
      .series()
      .errors(function(err) {
        error = true;
        callback(err);
      })
      .done(function() {
        if (!error) {
          callback();
        }
      });
  };

  this.close = function() {
    if (streams.pits) {
      streams.pits.close();
    }

    if (streams.relations) {
      streams.relations.close();
    }
  };

  return this;
};
