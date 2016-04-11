#!/usr/bin/env node

var fs = require('fs')
var util = require('util')
var path = require('path')
var mkdirp = require('mkdirp')
var chalk = require('chalk')
var H = require('highland')
var config = require('histograph-config')
var minimist = require('minimist')
var moment = require('moment')

var datasetWriter = require('./lib/dataset-writer')
var datasetReader = require('./lib/dataset-reader')

var argv = minimist(process.argv.slice(2))

const STATUS_FILENAME = '.status.json'

var readDir = H.wrapCallback(function (dir, callback) {
  return fs.readdir(dir, function (err, files) {
    var dirs = []
    if (!err) {
      dirs = files || []
    }

    dirs = files
      .filter(function (file) {
        return file.startsWith(config.data.modulePrefix)
      })
      .map(function (dir) {
        return dir.replace(config.data.modulePrefix, '')
      })

    callback(err, dirs)
  })
})

var readModule = function (d) {
  var module
  var meta
  var modulePath = path.join(config.data.moduleDir, config.data.modulePrefix + d, d.replace(config.data.modulePrefix, ''))
  var datasetPath = path.join(config.data.moduleDir, config.data.modulePrefix + d, util.format('%s.dataset.json', d))

  try {
    module = require(modulePath)
    meta = require(datasetPath)
  } catch (err) {
    var moduleNotFound = (err.code === 'MODULE_NOT_FOUND')

    if (moduleNotFound) {
      // See if module itself is not found, or a module required by the module!
      moduleNotFound = err.message.includes(modulePath)
    }

    if (moduleNotFound) {
      console.error(chalk.red('No data module: ') + d)
    } else {
      console.error(chalk.red('Error loading data module: ') + d)
    }
    console.log(chalk.gray(err.stack.split('\n').join('\n')))

    throw err
  }

  return {
    dataset: d,
    config: config.data.modules[d],
    meta: meta,
    module: module
  }
}

var ensureDir = function (dir) {
  mkdirp.sync(dir)
}

var readStatusFile = function (dir) {
  var filename = path.join(dir, STATUS_FILENAME)
  try {
    return require(filename)
  } catch (err) {
    return null
  }
}

var writeStatusFile = function (dir, success) {
  var filename = path.join(dir, STATUS_FILENAME)
  var status = {
    date: new Date().toISOString(),
    success: success
  }
  fs.writeFileSync(filename, JSON.stringify(status, null, 2) + '\n')
}

var wrapStep = function (step, config, dirs, tools, callback) {
  var done = false
  console.log(util.format('    %s %s', chalk.gray('executing:'), chalk.blue(step.name)))

  try {
    step(config, dirs, tools, (err) => {
      if (!done) {
        writeStatusFile(dirs.current, !err)
        console.log(util.format('    %s %s %s', chalk.gray('result:'), err ? chalk.red('error') : chalk.green('success'), err ? chalk.gray(err.message || err.stack || err) : ''))
        callback(err)
      }
      done = true
    })
  } catch (err) {
    writeStatusFile(dirs.current, false)
    callback(err)
  }
}

var logModuleTitle = function (d) {
  var gray = []

  if (d.meta) {
    if (d.meta.title) {
      gray.push(d.meta.title)
    }

    if (d.meta.website) {
      gray.push(chalk.underline(d.meta.website))
    }
  }

  console.log(util.format(' - %s %s', d.dataset, chalk.gray(gray.join(' - '))))
}

console.log('Using data modules in ' + chalk.underline(util.format('%s*', path.join(config.data.moduleDir, config.data.modulePrefix))))
console.log(chalk.gray(util.format('  Saving data to %s\n', chalk.underline(path.join(config.data.outputDir, '<step>')))))

if (argv._.length === 0) {
  var count = 0

  // List data modules - don't run anything
  readDir(config.data.moduleDir)
    .flatten()
    .map(readModule)
    // Errors are handled by readModule function
    .errors(() => {})
    .compact()
    .each(function (d) {
      logModuleTitle(d)

      const stepsPadding = '    '
      const stepsLabel = 'steps: '
      const stepsFirst = chalk.gray(stepsLabel)
      const stepsOther = new Array(stepsLabel.length + 1).join(' ');
      if (d.module.steps) {
        d.module.steps.forEach((step, i) => {
          const dir = path.join(config.data.outputDir, step.name, d.dataset)
          const status = readStatusFile(dir)

          var getStatusText = (text) => `${chalk.gray('(')}${text}${chalk.gray(')')}`
          var statusText

          if (status) {
            const fromNow = chalk.bold(moment(status.date).fromNow())
            const success = status.success ? chalk.green('success') : chalk.red('failed')
            statusText = getStatusText(chalk.gray(`last ran ${fromNow}: ${success}`))
          } else {
            statusText = getStatusText(chalk.yellow('never ran'))
          }
          console.log(`${stepsPadding}${i === 0 ? stepsFirst : stepsOther}${chalk.blue(step.name)} ${statusText}`)
        })
      } else {
        console.log(`${stepsPadding}${stepsFirst}${chalk.red('no steps found!')}`)
      }
      count += 1
    })
    .done(function () {
      if (!count) {
        console.log(chalk.red('No data modules found...'))
      }

      console.log('\nUsage: node index.js [--all] [--steps [step1,step2,...]] [--config /path/to/config.yml] [module ...]')
    })
} else {
  var exitCode = 1
  var errors = false
  process.on('exit', () => {
    writers.forEach((writer) => writer.close())
    if (exitCode === 0) {
      console.log('Done...')
    } else {
      console.log(chalk.red('Done, with errors...'))
      process.exit(exitCode)
    }
  })

  var writers = []

  H(argv._)
    .map(readModule)
    .compact()
    .map(function (d) {
      logModuleTitle(d)
      var steps = d.module.steps
      var argvSteps = []
      if (argv.steps) {
        argvSteps = argv.steps.split(',')
      }

      if (argvSteps.length) {
        var missingSteps = []
        var availableSteps = steps.map((step) => step.name)
        argvSteps.forEach((step) => {
          if (availableSteps.indexOf(step) === -1) {
            missingSteps.push(step)
          }
        })

        if (missingSteps.length) {
          throw new Error(`  Steps missing in data module: ${missingSteps.join(', ')}`)
        }
      }

      var stepDirs = {}
      steps.forEach((step) => {
        stepDirs[step.name] = path.join(config.data.outputDir, step.name, d.dataset)
      })

      return steps.map(function (step, i) {
        if (argv.steps && !(argvSteps.indexOf(step.name) > -1)) {
          // if steps command line argument is supplied, only execute steps in argv.steps
          // step.execute is the step's function to be executed,
          //   step.execute.name is the name of this function
          return null
        }

        var dir = path.join(config.data.outputDir, step.name, d.dataset)

        // Set directory of previous step
        var previousDir
        if (i > 0) {
          previousDir = path.join(config.data.outputDir, steps[i - 1].name, d.dataset)
        }

        ensureDir(dir)

        var tools = {
          writer: datasetWriter(d.dataset, dir, d.meta),
          reader: datasetReader
        }

        writers.push(tools.writer)

        var dirs = Object.assign({
          current: dir,
          previous: previousDir
        }, stepDirs)

        return H.curry(wrapStep, step, d.config, dirs, tools)
      })
    })
    .stopOnError((err) => {
      errors = true
      console.error(err)
    })
    .flatten()
    .compact()
    .nfcall([])
    .series()
    .errors((err) => {
      errors = true
      console.error(err)
    })
    .done(function () {
      if (!errors) {
        exitCode = 0
      }
    })
}
