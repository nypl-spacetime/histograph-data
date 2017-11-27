const fs = require('fs')
const path = require('path')
const R = require('ramda')

const schemas = require('spacetime-schemas')

const Validator = require('jsonschema').Validator
const validator = new Validator()

const STATUS_FILENAME = 'etl-results.json'

function readDir (config) {
  const moduleDir = config.etl.moduleDir
  const prefix = config.etl.modulePrefix

  return fs.readdirSync(moduleDir)
    .filter((file) => file.startsWith(prefix))
    .map((dir) => dir.replace(prefix, ''))
}

function readStatusFile (outputDir, id, step) {
  const filename = path.join(outputDir, step, id, STATUS_FILENAME)
  try {
    const status = require(filename)
    return R.omit(['dataset'], status)
  } catch (err) {
    return undefined
  }
}

function allModules (config) {
  return readDir(config)
    .map((moduleId) => readModule(config, moduleId))
}

function readModule (config, id) {
  const moduleDir = config.etl.moduleDir
  const prefix = config.etl.modulePrefix
  const outputDir = config.etl.outputDir

  let moduleConfig
  if (config.etl.modules && config.etl.modules[id]) {
    moduleConfig = config.etl.modules[id]
  }

  const dir = path.join(moduleDir, `${prefix}${id}`)
  const moduleFilename = path.join(dir, `${id}.js`)
  const datasetFilename = path.join(dir, `${id}.dataset.json`)
  const packageFilename = path.join(dir, 'package.json')

  try {
    const module = require(moduleFilename)
    const dataset = require(datasetFilename)
    const pkg = require(packageFilename)

    // Check if dataset descriptor is valid and contains all necessaryfields
    const validationResults = validator.validate(dataset, schemas.dataset)
    if (validationResults.errors.length) {
      const message = [
        'Errors in dataset description file:',
        `  Path: ${datasetFilename}`,
        '  Errors:',
        ...validationResults.errors
          .map((error) => error.message)
          .map((message) => `    - ${message}`)
      ].join('\n')

      throw new Error(message)
    }

    const steps = module.steps.map((fn) => fn.name)

    return {
      id,
      dir,
      module,
      package: pkg,
      steps,
      stepObj: R.fromPairs(module.steps.map((fn, index) => ([
        fn.name,
        {
          fn,
          index,
          outputDir: path.join(outputDir, fn.name, id)
        }
      ]))),
      stepOutputDirs: R.fromPairs(steps.map((step) => ([
        step,
        path.join(outputDir, step, id)
      ]))),
      dataset,
      config: moduleConfig,
      status: R.fromPairs(steps.map((step) => ([
        step,
        readStatusFile(config.etl.outputDir, id, step)
      ])))
    }
  } catch (err) {
    return {
      id,
      dir,
      err
    }
  }
}

module.exports = {
  readDir,
  allModules,
  readModule
}
