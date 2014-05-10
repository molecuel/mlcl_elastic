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
  var mongo;
  var testobjEn;
  var testobjDe;

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
    mongo = mlcl_database(molecuel);
    mlcl_elastic(molecuel);

    testobjEn = {
      '_id': '98098098',
      'title': 'test title',
      'lang': 'en',
      'url': '/test_title'
    };

    testobjDe = {
      '_id': '123456',
      'title': 'test title',
      'lang': 'de',
      'url': '/test_title'
    };
    done();
  });

  describe('elastic', function() {
    var searchcon;
    var dbcon;

    it('should initialize db connection', function(done) {
      molecuel.once('mlcl::database::connection:success', function(database) {
        dbcon = database;
        database.should.be.a.object;
        done();
      });
      molecuel.emit('mlcl::core::init:post', molecuel);
    });

    it('should initialize search connection', function(done) {
      molecuel.once('mlcl::search::connection:success', function(search) {
        searchcon = search;
        search.should.be.a.object;
        done();
      });
      molecuel.emit('mlcl::core::init:post', molecuel);
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

    it('should add german object to the index', function(done) {
      searchcon.index('test', testobjDe, function(error, result) {
        should.not.exists(error);
        result.should.be.a.object;
        done();
      });
    });

    it('should add english object to the index', function(done) {
      searchcon.index('test', testobjEn, function(error, result) {
        should.not.exists(error);
        result.should.be.a.object;
        done();
      });
    });

    it('should get german object by URL', function(done) {
      searchcon.getByUrl(testobjDe.url, testobjDe.lang, function(error, result) {
        should.not.exists(error);
        result.should.be.a.object;
        should.exists(result.hits.hits[0]);
        result.hits.hits[0].should.be.a.object;
        done();
      });
    });

    it('should get english object by URL', function(done) {
      searchcon.getByUrl(testobjEn.url, testobjEn.lang, function(error, result) {
        should.not.exists(error);
        result.should.be.a.object;
        should.exists(result.hits.hits[0]);
        result.hits.hits[0].should.be.a.object;
        done();
      });
    });

    it('should get a object by id', function(done) {
      searchcon.getById('123456', function(error, result) {
        should.not.exists(error);
        result.should.be.a.object;
        should.exists(result.hits.hits[0]);
        result.hits.hits[0].should.be.a.object;
        done();
      });
    });

    after(function(done) {
      searchcon.deleteIndex('mlcl-elastic-unit*', function(error) {
        should.not.exists(error);
        done();
      });
    });

  });
});
