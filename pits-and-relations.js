var fs = require('fs');
var path = require('path');
var config = require(process.env.HISTOGRAPH_CONFIG);
var geojsonhint = require('geojsonhint');
var validator = require('is-my-json-valid');

var validators = {
  pits: validator(fs.readFileSync(config.schemas.dir + '/json/pits.schema.json', 'utf-8')),
  relations: validator(fs.readFileSync(config.schemas.dir + '/json/relations.schema.json', 'utf-8'))
};

function createWriteStream(type, config) {
  return fs.createWriteStream(path.join(config.source, config.source + '.' + type + '.ndjson'), {
    flags: config.truncate === false ? 'r+' : 'w',
    encoding: 'utf8',
  });
}

module.exports = function(config) {

  var streams = {
    pits: createWriteStream('pits', config),
    relations: createWriteStream('relations', config)
  };

  this.emit = function(type, obj, callback) {
    var jsonValid = validators[type](obj);
    var valid = true;
    var errors;

    if (!jsonValid) {
      errors = validators[type].errors;
      valid = false;
    } else if (type === 'pits' && obj.geometry) {
      var geojsonErrors = geojsonhint.hint(obj.geometry);
      if (geojsonErrors.length > 0) {
        errors = geojsonErrors;
        valid = false;
      }
    }

    if (!valid) {
      setImmediate(callback, errors);
    } else {
      streams[type].write(JSON.stringify(obj) + '\n', function() {
        callback();
      });
    }
  };

  this.close = function() {
    streams.pits.close();
    streams.relations.close();
  };

  return this;
};
