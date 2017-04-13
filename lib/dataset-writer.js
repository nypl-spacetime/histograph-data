const fs = require('fs')
const path = require('path')
const schemas = require('spacetime-schemas')
const H = require('highland')
const geojsonhint = require('@mapbox/geojsonhint')
const validator = require('is-my-json-valid')

const Stats = require('./stats')

const validators = {
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

  let first = true
  let streams = {}

  let stats = Stats()

  function writeObject (data, callback) {
    if (!data) {
      setImmediate(callback)
      return
    }

    if (first) {
      let metaFilename = path.join(dir, `${dataset}.dataset.json`)
      fs.writeFileSync(metaFilename, JSON.stringify(meta, null, 2))
    }
    first = false

    let type = data.type
    let obj = data.obj

    if (type !== 'object' && type !== 'relation' && type !== 'log') {
      callback(new Error('type should be either `object`, `relation` or `log`'))
      return
    }

    let streamName
    let valid = true
    let errors

    if (type === 'log') {
      streamName = 'log'
    } else {
      // object => objects, relation => relations
      streamName = type + 's'

      let jsonValid = validators[streamName](obj)

      if (!jsonValid) {
        errors = {
          message: JSON.stringify(validators[streamName].errors),
          data: data.obj
        }
        valid = false
      } else if (type === 'object' && obj.geometry) {
        let geojsonErrors = geojsonhint.hint(obj.geometry)
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

      stats.add(type, obj)

      streams[streamName].write(JSON.stringify(obj) + '\n', callback)
    }
  }

  function writeObjects (data, callback) {
    if (!data || !data.length) {
      setImmediate(callback)
      return
    }

    let error = false

    H(data)
      .map(H.curry(this.writeObject))
      .flatten()
      .nfcall([])
      .series()
      .errors((err) => {
        error = true
        callback(err)
      })
      .done(() => {
        if (!error) {
          callback()
        }
      })
  }

  function close () {
    if (streams.objects) {
      streams.objects.close()
    }

    if (streams.relations) {
      streams.relations.close()
    }
  }

  return {
    writeObject,
    writeObjects,
    close,
    getStats: stats.getStats
  }
}
