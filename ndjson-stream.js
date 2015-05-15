var fs = require('fs');
var path = require('path');
var config = require(process.env.HISTOGRAPH_CONFIG);
var geojsonhint = require('geojsonhint');
var validator = require('is-my-json-valid');
var ndjson = require('ndjson');
var _ = require('highland');

var validators = {
  pits: validator(fs.readFileSync(config.schemas.dir + '/json/pits.schema.json', 'utf8')),
  relations: validator(fs.readFileSync(config.schemas.dir + '/json/relations.schema.json', 'utf8'))
};

function createWriteStream(type, config) {
  return fs.createWriteStream(path.join(config.source, config.source + '.' + type + '.ndjson'), {
    flags: config.truncate === false ? 'r+' : 'w',
    encoding: 'utf8',
  });
}

var EOL = require('os').EOL;

var JSON_LINES = function(obj) {
  console.log(obj)
  return JSON.stringify(obj) + EOL;
}

var filterStream = function(type, config) {

  var validGeojson = function(obj) {
    if (type !== 'pits' || !obj.geometry) {
      return true;
    }
    var errs = geojsonhint.hint(obj.geometry);
    return (errs.length == 0);
  };

  return _.pipeline(
    _.filter(validators[type]),
    _.filter(validGeojson),
    _.map(JSON_LINES)
  );
}

module.exports = function(stream, type, config){
  console.log(type)
  // stream.observe().map(_.log);

  return stream
    .pipe(filterStream(type, config))
    .pipe(createWriteStream(type, config));
}