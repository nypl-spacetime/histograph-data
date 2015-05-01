var path = require('path');
var async = require('async');
var config = require(process.env.HISTOGRAPH_CONFIG);
var sources = process.argv.slice(2);
require('colors');

var steps = [
  'download',
  'convert'
];

sources.forEach(function(source) {
  var importer = require('./' + path.join(source, source));

  console.log('Processing source ' + source.inverse + ':');
  async.eachSeries(steps, function(step, callback) {
    if (importer[step]) {
      console.log('  Executing step ' + step.underline + '...');
      importer[step](config.data[source], function(err) {
        if (err) {
          console.log('    Error: '.red + err);
        } else {
          console.log('    Done!'.green);
        }

        callback(err);
      });
    } else {
      callback();
    }
  },

  function(err) {
    if (err) {
      console.log('Error: '.red + err);
    } else {
      console.log('\nAll sources done!'.green.underline);
    }
  });
});
