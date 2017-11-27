const fs = require('fs')
const path = require('path')
const schemas = require('spacetime-schemas')
const H = require('highland')
const geojsonhint = require('@mapbox/geojsonhint')

const Stats = require('./stats')

const Validator = require('jsonschema').Validator
const validator = new Validator()

function createValidator (schema) {
  return function (instance) {
    return validator.validate(instance, schema)
  }
}

const validators = {
  objects: createValidator(schemas.objects),
  relations: createValidator(schemas.relations)
}

module.exports = function (dataset, dir, meta) {
  let dataValidator
  if (meta.fields) {
    dataValidator = createValidator(meta.fields)
  }

  function createWriteStream (type) {
    return fs.createWriteStream(path.join(dir, dataset + '.' + type + '.ndjson'), {
      flags: 'w',
      encoding: 'utf8'
    })
  }

  let first = true
  const counts = {
    relations: 0,
    objects: 0,
    logs: 0
  }

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

    const type = data.type
    const obj = data.obj

    if (type !== 'object' && type !== 'relation' && type !== 'log') {
      callback(new Error('type should be either `object`, `relation` or `log`'))
      return
    }

    let valid = true
    let errors = []

    // object => objects, relation => relations, log => logs
    const streamName = type + 's'

    counts[streamName] += 1

    if (streamName !== 'logs') {
      const validationResults = validators[streamName](obj)

      if (validationResults.errors.length) {
        errors = validationResults.errors.map((error) => error.message)
        valid = false
      } else if (type === 'object') {
        if (obj.data && dataValidator) {
          let dataValidationResults

          try {
            dataValidationResults = dataValidator(obj.data)
          } catch (err) {
            errors = [
              ...errors,
              err.message
            ]
            valid = false
          }

          if (dataValidationResults && dataValidationResults.errors.length) {
            errors = [
              ...errors,
              ...dataValidationResults.errors
                .map((error) => `data object: ${error}`)
            ]
            valid = false
          }
        }

        if (obj.geometry) {
          const geojsonErrors = geojsonhint.hint(obj.geometry)
          if (geojsonErrors.length > 0) {
            // If all geojsonErrors are of level 'message', ignore geojsonhint
            const allMessages = geojsonErrors.reduce((acc, val) => acc && val.level === 'message', true)

            if (!allMessages) {
              errors = [
                ...errors,
                ...geojsonErrors
                  .map((error) => `GeoJSON error: ${error.message}`)
              ]

              valid = false
            }
          }
        }
      }
    }

    if (!valid) {
      const message = [
        'Error writing data:',
        ...errors.map((error) => `  - ${error}`),
        `Caused by ${type} #${counts[streamName]}:`,
        JSON.stringify(obj)
      ].join('\n')

      setImmediate(callback, new Error(message))
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

