var fs = require('fs');
var SparqlClient = require('sparql-client');
var async = require('async');

// TGN configuration
var sparqlFile = 'tgn-terms';
var sparqlEndpoint = 'http://vocab.getty.edu/sparql.rdf';

exports.download = function(config, callback) {
  async.eachSeries(config.args, function(args, callback) {
    var client = new SparqlClient(sparqlEndpoint);
    var sparqlQuery = fs.readFileSync(__dirname + '/' + sparqlFile + '.sparql', 'utf8');

    Object.keys(args).forEach(function(arg) {
      sparqlQuery = sparqlQuery.replace('{{ ' + arg + ' }}', args[arg]);
    });

    client.query(sparqlQuery)
      .execute(function(error, results) {
        fs.writeFileSync(__dirname + '/' + sparqlFile + '.xml', results);
        callback();
      });
  },

  function(err) {
    callback(err);
  });
};
