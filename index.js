var fs = require('fs')
var util = require('util')
var path = require('path')
var mkdirp = require('mkdirp')
var chalk = require('chalk')
var H = require('highland')
var config = require('histograph-config')
var minimist = require('minimist')

var datasetWriter = require('./lib/dataset-writer')
var datasetReader = require('./lib/dataset-reader')

var argv = minimist(process.argv.slice(2))

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

    return null
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

var wrapStep = function (step, config, dir, writer, callback) {
  var done = false
  console.log(util.format('    %s %s', chalk.gray('executing:'), chalk.blue(step.name)))

  try {
    step(config, dir, writer, function (err) {
      if (!done) {
        console.log(util.format('    %s %s %s', chalk.gray('result:'), err ? chalk.red('error') : chalk.green('success'), err ? chalk.gray(err.message || err.stack || err) : ''))
        callback(err)
      }
      done = true
    })
  } catch (err) {
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
console.log(chalk.gray(util.format('  Saving data to %s\n', chalk.underline(config.data.outputDir))))

if (argv._.length === 0) {
  var count = 0

  // List data modules - don't run anything
  readDir(config.data.moduleDir)
    .flatten()
    .map(readModule)
    .compact()
    .each(function (d) {
      logModuleTitle(d)

      var stepsMessage
      if (d.module.steps) {
        // stepsMessage = chalk.blue((d.module.steps).map(function (f) { return f.name; }).join(', '))
        stepsMessage = d.module.steps
          .map((step) => chalk.blue(step.name))
          .join(chalk.gray(', '))
      } else {
        stepsMessage = chalk.red('no steps found!')
      }

      console.log(util.format('    %s %s', chalk.gray('steps:'), stepsMessage))

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
        var availableSteps = steps.map(step => step.name)
        argvSteps.forEach(step => {
          if (availableSteps.indexOf(step) === -1) {
            missingSteps.push(step)
          }
        })

        if (missingSteps.length) {
          throw `  Steps missing in data module: ${missingSteps.join(', ')}`
        }
      }

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

        var dirs = {
          current: dir,
          previous: previousDir
        }

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
