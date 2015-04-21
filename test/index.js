/**
 * Created by dob on 20.11.13.
 */
var should = require('should'),
    assert = require('assert'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    mlcl_database = require('mlcl_database'),
    mlcl_queue = require('mlcl_queue'),
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

        molecuel.log = {};
        molecuel.log.info = console.log;
        molecuel.log.error = console.log;
        molecuel.log.debug = console.log;
        molecuel.log.warn = console.log;

        molecuel.config = {};

        molecuel.config.queue = {
          uri: 'amqp://localhost'
        };

        if(process.env.NODE_ENV === 'dockerdev') {
          molecuel.config.queue = {
            uri: 'amqp://192.168.99.100'
          };
        }

        molecuel.config.search = {
            hosts: ['http://localhost:9200'],
            prefix: 'mlcl-elastic-unit'
        };
        molecuel.config.database = {
            type: 'mongodb',
            uri: 'mongodb://localhost/mlcl-elastic-unit'
        };

        molecuel.config.queue = {
            uri: 'amqp://localhost'
        };

        if (process.env.NODE_ENV === 'dockerdev') {
            molecuel.config.queue = {
                uri: 'amqp://192.168.99.100'
            };
        }

        mongo = mlcl_database(molecuel);
        mlcl_elastic(molecuel);
        mlcl_queue(molecuel);

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
        var model;
        var mongoobj;

        it('should initialize db and search connection', function(done) {

            var mycheck = function() {
                if(searchcon && dbcon) {
                    done();
                }
            };
            molecuel.once('mlcl::database::connection:success',
                function(
                    database) {
                    dbcon = database;
                    database.should.be.a.object;
                    mycheck();
                });

            molecuel.once('mlcl::search::connection:success',
                function(search) {
                    searchcon = search;
                    search.should.be.a.object;
                    mycheck();
                });

            molecuel.emit('mlcl::core::init:post', molecuel);
        });

        it('should initialize schema plugin', function(done) {
            var Schema = dbcon.database.Schema;
            var testSchema = new Schema({
                name: {
                    type: String
                },
                url: {
                    type: String,
                    elastic: {
                        mapping: {
                            type: 'string',
                            index: 'not_analyzed'
                        }
                    }
                },
                lang: {
                    type: String
                }
            });
            molecuel.on('mlcl::elastic::registerPlugin:post',
                function(el,
                    modelname, schema) {
                    schema.methods.search.should.be.a.

                    function;
                    schema.statics.searchById.should.be.a.

                    function;
                    schema.statics.searchByUrl.should.be.a.

                    function;
                    setTimeout(function() {
                        done();
                    }, 1000);
                });
            model = dbcon.registerModel('test', testSchema, {
                indexable: true
            });
        });

        it('should add german object to the index', function(done) {
            searchcon.index('test', testobjDe, function(error,
                result) {
                should.not.exists(error);
                result.should.be.a.object;
                done();
            });
        });

        it('should add english object to the index', function(done) {
            searchcon.index('test', testobjEn, function(error,
                result) {
                should.not.exists(error);
                result.should.be.a.object;
                // timeout before reading the data
                setTimeout(function() {
                    done();
                }, 1000);
            });
        });

        it('should get german object by URL', function(done) {
            searchcon.searchByUrl(testobjDe.url, testobjDe.lang,
                function(error,
                    result) {
                    should.not.exists(error);
                    result.should.be.a.object;
                    should.exists(result.hits.hits[0]);
                    result.hits.hits[0].should.be.a.object;
                    done();
                });
        });

        it('should get english object by URL', function(done) {
            searchcon.searchByUrl(testobjEn.url, testobjEn.lang,
                function(error,
                    result) {
                    should.not.exists(error);
                    result.should.be.a.object;
                    should.exists(result.hits.hits[0]);
                    result.hits.hits[0].should.be.a.object;
                    done();
                });
        });

        it('should get a object by id', function(done) {
            searchcon.searchById('123456', function(error, result) {
                should.not.exists(error);
                result.should.be.a.object;
                should.exists(result.hits.hits[0]);
                result.hits.hits[0].should.be.a.object;
                done();
            });
        });

        it('should save to mongodb', function(done) {
            mongoobj = new model({
                name: 'testname',
                url: '/mytesturl',
                lang: 'de'
            });

            mongoobj.save(function(error, result) {
                should.not.exists(error);
                result.should.be.a.object;
                // timeout before reading the data
                setTimeout(function() {
                    done();
                }, 1000);
            });
        });

        it('should search by db connection static function', function(
            done) {
            searchcon.searchByUrl(mongoobj.url, mongoobj.lang,
                function(error,
                    result) {
                    should.not.exists(error);
                    result.should.be.a.object;
                    should.exists(result.hits.hits[0]);
                    result.hits.hits[0].should.be.a.object;
                    done();
                });
        });

        it('should create a new index with a mapping', function(done) {
            var mapping = {
                'tweet': {
                    '_ttl': {
                        'enabled': true,
                        'default': '10s'
                    }
                }
            };
            searchcon.checkCreateIndex('tweet', {}, mapping,
                function(error) {
                    should.not.exists(error);
                    done();
                });
        });

        it('should read the created mapping', function(done) {
            searchcon.getMapping('tweet', function(error, result) {
                should.not.exists(error);
                result['mlcl-elastic-unit-tweet'].mappings.tweet
                    ._ttl.enabled.should
                    .be.ok;
                done();
            });
        });

        it('should save with ttl', function(done) {
            searchcon.index('tweet', testobjDe, function(error,
                result) {
                should.not.exists(error);
                result.should.be.a.object;
                done();
            });
        });

        it('should find the ttl object', function(done) {
            searchcon.search({
                index: 'tweet'
            }, function(error, result) {
                should.not.exists(error);
                result.should.be.a.object;
                done();
            });
        });

        it('should return the correct prefix', function(done) {
            assert.equal(searchcon.getIndexName('model'),
                'mlcl-elastic-unit-model');
            assert.equal(searchcon.getIndexName(
                    'mlcl-elastic-unit-model'),
                'mlcl-elastic-unit-model');
            done();
        });

        it('should resync all elements', function(done) {
            model.resync('test');
            setTimeout(function() {
                done();
            }, 1000);
        });

        after(function(done) {
            dbcon.database.connection.db.dropDatabase(function(
                error) {
                should.not.exists(error);
                searchcon.deleteIndex('*', function(error) {
                    should.not.exists(error);
                    done();
                });
            });
        });
    });
});
