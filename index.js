#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const H = require('highland')
const mkdirp = require('mkdirp')
const config = require('spacetime-config')

const modules = require('./lib/modules')
const logging = require('./lib/logging')
const datasetWriter = require('./lib/dataset-writer')

const STATUS_FILENAME = 'etl-results.json'

function ensureDir (dir) {
  mkdirp.sync(dir)
}

function checkConfig (config) {
  if (!config || !config.etl) {
    throw new Error('Configuration file should have \'etl\' section')
  }

  const etlConfig = config.etl

  const checkKey = (key) => {
    if (!etlConfig[key]) {
      throw new Error(`'${key}' missing in 'etl' section of configuration file`)
    }
  }

  checkKey('moduleDir')
  checkKey('modulePrefix')
  checkKey('outputDir')
}

checkConfig(config)

function executeStep (module, step, log, callback) {
  const stepFn = module.stepObj[step].fn
  const currentDir = module.stepObj[step].outputDir

  if (log) {
    logging.stepStart(step)
  }

  // Set directory of previous step
  let previousDir
  const stepIndex = module.stepObj[step].index
  if (stepIndex > 0) {
    const previousStep = module.steps[stepIndex - 1]
    previousDir = module.stepObj[previousStep].outputDir
  }

  try {
    ensureDir(currentDir)
  } catch (err) {
    callback(err)
    return
  }

  const tools = {
    writer: datasetWriter(module.id, currentDir, module.dataset)
  }

  const outputDir = config.etl.outputDir
  const dirs = Object.assign({
    getDir: (datasetId, step) => path.join(outputDir, step, datasetId),
    current: currentDir,
    previous: previousDir
  }, module.stepOutputDirs)

  try {
    let done = false

    stepFn(module.config, dirs, tools, (err) => {
      if (!done) {
        if (log) {
          logging.stepDone(err)
        }

        tools.writer.close()

        if (err) {
          writeStatusFile(module, step, currentDir, err)
          callback(err)
        } else {
          writeStatusFile(module, step, currentDir, err, tools.writer.getStats())
          callback()
        }
      }

      done = true
    })
  } catch (err) {
    writeStatusFile(module, step, currentDir, err)
    callback(err)
  }
}

function writeStatusFile (module, step, dir, err, stats) {
  const filename = path.join(dir, STATUS_FILENAME)
  const status = {
    step,
    date: new Date().toISOString(),
    success: err === undefined,
    dataset: module.dataset,
    stats
  }

  fs.writeFileSync(filename, JSON.stringify(status, null, 2) + '\n')
}

function parseCommand (command) {
  if (!command.length || command.endsWith('.')) {
    throw new Error(`Incorrect command: '${command}' - should be of form 'moduleId[.step]'`)
  }

  const parts = command.split('.')
  return {
    moduleId: parts[0],
    step: parts[1]
  }
}

function execute (command, callback, log) {
  let moduleId
  let step
  try {
    ({moduleId, step} = parseCommand(command))
  } catch (err) {
    callback(err)
    return
  }

  const module = modules.readModule(config, moduleId)

  if (module.err) {
    callback(module.err)
  } else {
    if (log) {
      logging.logModuleTitle(module)
    }

    if (step) {
      if (module.steps.includes(step)) {
        executeStep(module, step, log, callback)
      } else {
        callback(new Error(`Module ${moduleId} does not contain step '${step}'`))
      }
    } else {
      let errors = false

      H(module.steps)
        .map((step) => H.curry(executeStep, module, step, log))
        .nfcall()
        .series()
        .stopOnError((err) => {
          errors = true
          callback(err)
        })
        .done(() => {
          if (!errors) {
            callback()
          }
        })
    }
  }
}

if (require.main === module) {
  const minimist = require('minimist')
  const argv = minimist(process.argv.slice(2))

  let exitCode = 1
  let errors = false
  process.on('exit', () => process.exit(exitCode))

  if (argv._.length === 0) {
    let count = 0

    logging.logModulesStart(config)

    H(modules.readDir(config))
      .map((moduleId) => modules.readModule(config, moduleId))
      .map((module) => logging.logModule(module, config.etl.modulePath))
      .each(() => count++)
      .done(() => {
        logging.logModulesEnd(count)
      })
  } else {
    const reorderedExecute = (command, log, callback) => execute(command, callback, log)

    H(argv._)
      .map((command) => H.curry(reorderedExecute, command, true))
      .nfcall([])
      .series()
      .stopOnError((err) => {
        errors = true
        logging.stepsDone(err)
      })
      .done(() => {
        if (!errors) {
          logging.stepsDone()
        }
      })
  }
}

module.exports = execute
