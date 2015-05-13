var fs = require('fs');
var path = require('path');
var config = require(process.env.HISTOGRAPH_CONFIG);
var geojsonhint = require('geojsonhint');
var validator = require('is-my-json-valid');
var ndjson = require('ndjson');

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

var _ = require('highland');

module.exports = function streamFor(type, config)
{
  var valid_geojson = function(obj){
      if(type !== 'pits' || !obj.geometry)
        return true;

      var errs = geojsonhint.hint(obj.geometry);
      return (errs.length == 0);
  };

  return _.pipeline(
    _.filter(validators[type]),
    _.filter(valid_geojson),
    ndjson.serialize(),
    _(createWriteStream(type, config))
  );
}
