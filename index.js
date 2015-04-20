/// <reference path="./typings/node/node.d.ts"/>
/// <reference path="./typings/mongolastic/mongolastic.d.ts"/>
/// <reference path="./typings/async/async.d.ts"/>
var mongolastic = require('mongolastic');
var mlcl_elastic = (function () {
    function mlcl_elastic() {
        var _this = this;
        if (mlcl_elastic._instance) {
            throw new Error("Error: Instantiation failed. Singleton module! Use .getInstance() instead of new.");
        }
        this.log = console.log;
        mlcl_elastic._instance = this;
        mlcl_elastic.molecuel.on('mlcl::core::init:post', function (molecuel) {
            _this.config = molecuel.config.search;
            if (molecuel.log) {
                _this.log = molecuel.log;
            }
        });
        mlcl_elastic.molecuel.on('mlcl::queue::init:post', function (queue) {
            _this.queue = queue;
            _this.connect(function (err, connection) {
                if (err) {
                    _this.log('mlcl_elastic', 'Error while connecting' + err);
                }
                else {
                    _this.connection = connection;
                    mlcl_elastic.molecuel.emit('mlcl::search::connection:success', _this);
                }
            });
        });
        mlcl_elastic.molecuel.on('mlcl::database::registerModel:pre', function (database, modelname, schema, options) {
            if (options.indexable) {
                options.modelname = modelname;
                schema.plugin(_this.plugin, options);
                mlcl_elastic.molecuel.emit('mlcl::elastic::registerPlugin:post', _this, modelname, schema);
            }
        });
        mlcl_elastic.molecuel.on('mlcl::database::registerModel:post', function (database, modelname, model) {
            mongolastic.registerModel(model, function (err) {
                if (err) {
                    _this.log('mlcl_elastic', 'Error while registering model to elasticsearch' + err);
                }
                else {
                    if (mlcl_elastic.molecuel.serverroles && mlcl_elastic.molecuel.serverroles.worker) {
                        var qname = 'mlcl::elastic::' + modelname + ':resync';
                        var chan = _this.queue.getChannel();
                        chan.then(function (ch) {
                            ch.assertQueue(qname);
                            ch.prefetch(50);
                            ch.consume(qname, function (msg) {
                                var id = msg.content.toString();
                                if (id) {
                                    model.syncById(id, function (err) {
                                        if (!err) {
                                            ch.ack(msg);
                                        }
                                        else {
                                            mlcl_elastic.molecuel.log(err);
                                            ch.nack(msg);
                                        }
                                    });
                                }
                                else {
                                    ch.ack(msg);
                                }
                            });
                        }).then(null, function (err) {
                            mlcl_elastic.molecuel.log(err);
                        });
                    }
                }
            });
        });
    }
    mlcl_elastic.getInstance = function () {
        if (mlcl_elastic._instance === null) {
            mlcl_elastic._instance = new mlcl_elastic();
        }
        return mlcl_elastic._instance;
    };
    mlcl_elastic.init = function (m) {
        mlcl_elastic.molecuel = m;
        return mlcl_elastic.getInstance();
    };
    mlcl_elastic.prototype.connect = function (callback) {
        mongolastic.connect(this.config.prefix, this.config, callback);
    };
    mlcl_elastic.prototype.ensureIndex = function (modelname, callback) {
        var _this = this;
        mongolastic.indices.exists(function (modelname, err, exists) {
            if (!exists) {
                var mappings = {};
                mappings[modelname] = {
                    properties: {
                        url: {
                            type: 'string',
                            index: 'not_analyzed'
                        },
                        'location': {
                            'properties': {
                                geo: {
                                    type: 'geo_point',
                                    'lat_lon': true
                                }
                            }
                        }
                    }
                };
                var settings = {};
                mongolastic.indices.create(modelname, settings, mappings, function (err) {
                    if (err) {
                        _this.log('mlcl_elastic', 'Error while creating indices' + err);
                    }
                });
                callback();
            }
            else {
                callback();
            }
        });
    };
    mlcl_elastic.prototype.index = function (modelname, entry, callback) {
        mongolastic.index(modelname, entry, callback);
    };
    mlcl_elastic.prototype.delete = function (modelname, entry, callback) {
        mongolastic.delete(modelname, entry, callback);
    };
    mlcl_elastic.prototype.deleteIndex = function (modelname, callback) {
        mongolastic.deleteIndex(modelname, callback);
    };
    mlcl_elastic.prototype.sync = function (model, modelname, callback) {
        mongolastic.sync(model, modelname, callback);
    };
    mlcl_elastic.prototype.resync = function (modelname) {
        var elast = mlcl_elastic.getInstance();
        var dbmodel = this;
        if (modelname) {
            var queuename = 'mlcl::elastic::' + modelname + ':resync';
            var chan = elast.queue.getChannel();
            chan.then(function (ch) {
                ch.assertQueue(queuename);
                var stream = dbmodel.find({}, '_id').stream();
                stream.on('data', function (obj) {
                    ch.sendToQueue(queuename, new Buffer(obj._id.toString()));
                });
                stream.on('end', function () {
                    elast.log(new Date());
                    elast.log('reindex for ' + modelname + ' has been added to queue');
                });
            }).then(null, function (err) {
                elast.log(err);
            });
        }
    };
    mlcl_elastic.prototype.search = function (query, callback) {
        var elast = mlcl_elastic.getInstance();
        if (query && query.index) {
            query.index = elast.getIndexName(query.index);
        }
        mongolastic.search(query, callback);
    };
    mlcl_elastic.prototype.searchByUrl = function (url, lang, callback) {
        mlcl_elastic.getInstance().search({
            body: {
                from: 0,
                size: 1,
                filter: {
                    and: [
                        {
                            term: {
                                url: url
                            }
                        },
                        {
                            or: [
                                {
                                    term: {
                                        lang: lang
                                    }
                                },
                                {
                                    missing: {
                                        field: 'lang'
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        }, callback);
    };
    mlcl_elastic.prototype.searchById = function (id, callback) {
        mlcl_elastic.getInstance().search({
            body: {
                query: {
                    filtered: {
                        filter: {
                            term: {
                                _id: id
                            }
                        }
                    }
                }
            }
        }, callback);
    };
    mlcl_elastic.prototype.exists = function (indexname, callback) {
        this.connection.indices.exists({
            index: this.getIndexName(indexname),
        }, callback);
    };
    mlcl_elastic.prototype.create = function (indexname, settings, mappings, callback) {
        var elast = mlcl_elastic.getInstance();
        elast.connection.indices.create({
            index: elast.getIndexName(indexname),
            body: {
                settings: settings,
                mappings: mappings
            }
        }, callback);
    };
    mlcl_elastic.prototype.checkCreateIndex = function (indexname, settings, mappings, callback) {
        var elast = mlcl_elastic.getInstance();
        elast.exists(indexname, function (err, response) {
            if (!response) {
                elast.create(indexname, settings, mappings, function (err) {
                    callback(err, true);
                });
            }
            else {
                callback(err, false);
            }
        });
    };
    mlcl_elastic.prototype.getMapping = function (indexname, callback) {
        var elast = mlcl_elastic.getInstance();
        elast.connection.indices.getMapping({
            index: elast.getIndexName(indexname)
        }, callback);
    };
    mlcl_elastic.prototype.plugin = function (schema, options) {
        schema.plugin(mongolastic.plugin, options);
        var mylastic = mlcl_elastic.getInstance();
        schema.statics.searchByUrl = mylastic.searchByUrl;
        schema.statics.searchById = mylastic.searchById;
        schema.statics.resync = mylastic.resync;
        schema.methods.searchByUrl = mylastic.searchByUrl;
        schema.methods.searchById = mylastic.searchById;
    };
    mlcl_elastic.prototype.getIndexName = function (name) {
        var elast = mlcl_elastic.getInstance();
        if (elast.config.prefix) {
            if (name.indexOf(elast.config.prefix + '-') === 0) {
                return name.toLowerCase();
            }
            else {
                return elast.config.prefix + '-' + name.toLowerCase();
            }
        }
        else {
            return name.toLowerCase();
        }
    };
    mlcl_elastic._instance = null;
    return mlcl_elastic;
})();
module.exports = mlcl_elastic.init;
