var fs = require('fs');
var util = require('util');
var path = require('path');
var mkdirp = require('mkdirp');
var chalk = require('chalk');
var H = require('highland');
var config = require('histograph-config');
var minimist = require('minimist');
var datasetWriter = require('./dataset-writer');

var argv = minimist(process.argv.slice(2));

var readDir = H.wrapCallback(function(dir, callback) {
  return fs.readdir(dir, function(err, files) {
    var dirs = [];
    if (!err) {
      dirs = files || [];
    }

    dirs = files
      .filter(function(file) {
        return file.startsWith(config.data.modulePrefix);
      })
      .map(function(dir) {
        return dir.replace(config.data.modulePrefix, '');
      });

    callback(err, dirs);
  });
});

var readModule = function(d) {
  var module;

  try {
    module = require(path.join(config.data.baseDir, config.data.modulePrefix + d, d.replace(config.data.modulePrefix, '')));
  } catch (err) {
    console.error(chalk.red('Error reading data module: ') + d);
    console.log(chalk.gray(err.stack.split('\n').join('\n')));
    return null;
  }

  return {
    dataset: d,
    config: config.data.modules[d],
    module: module
  };
};

var ensureDir = function(d) {
  var dir = path.join(config.data.generatedDir, d.dataset);
  mkdirp.sync(dir);
  return d;
};

var copyDatasetFile = function(d) {
  var source = path.join(config.data.baseDir, config.data.modulePrefix + d.dataset, util.format('%s.dataset.json', d.dataset));
  var target = path.join(config.data.generatedDir, d.dataset, util.format('%s.dataset.json', d.dataset));
  fs.createReadStream(source).pipe(fs.createWriteStream(target));
  return d;
};

var wrapStep = function(step, config, dir, writer, callback) {
  console.log(util.format('    %s %s', chalk.gray('executing:'), chalk.blue(step.name)));
  step(config, dir, writer, function(err) {
    console.log(util.format('    %s %s %s', chalk.gray('result:'), err ? chalk.red('error') : chalk.green('success'), err ? chalk.gray(JSON.stringify(err)) : ''));

    callback();
  });
};

var logModuleTitle = function(d) {
  console.log(util.format(' - %s %s', d.dataset, chalk.gray(d.module.title + ' - ' + chalk.underline(d.module.url))));
};

console.log('Using data modules in ' + chalk.underline(util.format('%s%s*', config.data.baseDir, config.data.modulePrefix)));
console.log(chalk.gray(util.format('  Saving data to %s\n', chalk.underline(config.data.generatedDir))));

if (argv._.length === 0) {
  var count = 0;

  // List data modules - don't run anything
  readDir(config.data.baseDir)
    .flatten()
    .map(readModule)
    .compact()
    .each(function(d) {
      logModuleTitle(d);
      console.log(util.format('    %s %s', chalk.gray('steps:'), chalk.blue((d.module.steps || []).map(function(f) { return f.name; }).join(', '))));
      count += 1;
    })
    .done(function() {
      if (!count) {
        console.log(chalk.red('No data modules found...'));
      }

      console.log('\nUsage: node index.js [--all] [--steps [step1,step2,...]] [--config /path/to/config.yml] [module ...]');
    });
} else {
  var writers = [];

  H(argv._)
    .map(readModule)
    .compact()
    .map(ensureDir)
    .map(copyDatasetFile)
    .map(function(d) {
      logModuleTitle(d);

      var dir = path.join(config.data.generatedDir, d.dataset);
      var steps = d.module.steps;

      if (argv.steps) {
        steps = steps.filter(function(step) {
          return argv.steps.indexOf(step.name) > -1;
        });
      }

      var writer = datasetWriter(d.dataset, dir);
      writers.push(writer);

      return steps.map(function(step) {
        return H.curry(wrapStep, step, d.config, dir, writer);
      });
    })
    .flatten()
    .nfcall([])
    .series()
    .done(function() {
      writers.forEach(function(writer) {
        writer.close();
      });

      console.log('Done...');
    });
}
