var fs = require('fs')
var path = require('path')
var schemas = require('spacetime-schemas')
var H = require('highland')
var geojsonhint = require('geojsonhint')
var validator = require('is-my-json-valid')

var validators = {
  objects: validator(schemas.objects),
  relations: validator(schemas.relations)
}

module.exports = function (dataset, dir, meta) {
  function createWriteStream (type) {
    return fs.createWriteStream(path.join(dir, dataset + '.' + type + '.ndjson'), {
      flags: 'w',
      encoding: 'utf8'
    })
  }

  var first = true
  var streams = {}

  this.writeObject = (data, callback) => {
    if (first) {
      var metaFilename = path.join(dir, `${dataset}.dataset.json`)
      fs.writeFileSync(metaFilename, JSON.stringify(meta, null, 2))
    }
    first = false

    var type = data.type
    var obj = data.obj

    if (type !== 'object' && type !== 'relation' && type !== 'log') {
      callback(new Error('type should be either `object`, `relation` or `log`'))
      return
    }

    var streamName
    var valid = true

    if (type === 'log') {
      streamName = 'log'
    } else {
      // object => objects, relation => relations
      streamName = type + 's'

      var jsonValid = validators[streamName](obj)
      var errors

      if (!jsonValid) {
        errors = {
          message: JSON.stringify(validators[streamName].errors),
          data: data.obj
        }
        valid = false
      } else if (type === 'object' && obj.geometry) {
        var geojsonErrors = geojsonhint.hint(obj.geometry)
        if (geojsonErrors.length > 0) {
          errors = {
            message: JSON.stringify(geojsonErrors),
            data: JSON.stringify(data.obj)
          }
          valid = false
        }
      }
    }

    if (!valid) {
      setImmediate(callback, errors)
    } else {
      if (!streams[streamName]) {
        streams[streamName] = createWriteStream(streamName)
      }

      streams[streamName].write(JSON.stringify(obj) + '\n', function () {
        callback()
      })
    }
  }

  this.writeObjects = (data, callback) => {
    var error = false

    H(data)
      .map(H.curry(this.writeObject))
      .flatten()
      .nfcall([])
      .series()
      .errors(function (err) {
        error = true
        callback(err)
      })
      .done(function () {
        if (!error) {
          callback()
        }
      })
  }

  this.close = () => {
    if (streams.objects) {
      streams.objects.close()
    }

    if (streams.relations) {
      streams.relations.close()
    }
  }

  return this
}
