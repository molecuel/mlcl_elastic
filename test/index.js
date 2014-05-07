/**
 * Created by dob on 20.11.13.
 */
var should = require('should'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter,
  mlcl_database = require('mlcl_database'),
  mlcl_elastic = require('../');

describe('mlcl_elastic', function() {
  var mlcl;
  var molecuel;

  before(function(done) {
    // init fake molecuel
    mlcl = function() {
      return this;
    };
    util.inherits(mlcl, EventEmitter);
    molecuel = new mlcl();

    molecuel.config = { };
    molecuel.config.search = {
      host: 'localhost',
      port: '9200',
      prefix: 'mlcl-elastic-unit'
    };
    molecuel.config.database = {
      type: 'mongodb',
      uri: 'mongodb://localhost/mlcl-elastic-unit'
    };
    mlcl_database(molecuel);
    mlcl_elastic(molecuel);
    done();
  });

  describe('elastic', function() {
    var searchcon;
    var dbcon;
    it('should initialize search connection', function(done) {
      molecuel.emit('mlcl::core::init:post', molecuel);
      molecuel.once('mlcl::search::connection:success', function(search) {
        searchcon = search;
        search.should.be.a.object;
        done();
      });
    });

    it('should initialize db connection', function(done) {
      molecuel.once('mlcl::database::connection:success', function(database) {
        dbcon = database;
        database.should.be.a.object;
        done();
      });
    });

    it('should initialize schema plugin', function(done) {
      var Schema = dbcon.database.Schema;
      var testSchema = new Schema({
        name: {type: String}
      });
      var model = dbcon.registerModel('test', testSchema, {indexable:true});
      model.schema.methods.search.should.be.a.function;
      done();
    });
  });
});
