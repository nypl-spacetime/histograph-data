var fs = require('fs')
var path = require('path')
var H = require('highland')
var config = require('spacetime-config')

module.exports = (dataset, type) => {
  var filename = path.join(config.api.dataDir, 'datasets', dataset, `${type}.ndjson`)

  return H(fs.createReadStream(filename))
    .split()
    .compact()
    .map(JSON.parse)
}
