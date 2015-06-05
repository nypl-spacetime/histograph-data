var path = require('path');
var async = require('async');
var config = require(process.env.HISTOGRAPH_CONFIG);
var parseArgs = require('minimist');
require('colors');

var steps = [
  'download',
  'convert',
  'done'
];

var argv = parseArgs(process.argv.slice(2));

// By default, import all sources with have data config in configuration file
var sources = Object.keys(config.data);
if (argv._.length > 0) {
  sources = argv._;
}

async.eachSeries(sources, function(source, callback) {
  var importer = require('./' + path.join(source, source));

  console.log('Processing source ' + source.inverse + ':');
  async.eachSeries(steps, function(step, callback) {
    if (!argv.steps || (argv.steps && argv.steps.split(',').indexOf(step) > -1) || step === 'done') {
      if (importer[step]) {
        console.log('  Executing step ' + step.underline + '...');

        importer[step](config.data[source], function(err) {
          if (err) {
            console.log('    Error: '.red + JSON.stringify(err));
          } else {
            console.log('    Done!'.green);
          }

          callback(err);
        });
      } else {
        callback();
      }
    } else {
      if (importer[step]) {
        console.log(('  Skipping step ' + step.underline + '...').gray);
      }

      callback();
    }
  },

  function(err) {
    if (err) {
      console.log('Error: '.red + err);
    }

    callback();
  });
},

function() {
  console.log('\nAll sources done!'.green.underline);
});

