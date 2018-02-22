"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const mongolastic = require("mongolastic");
class mlcl_elastic {
    constructor() {
        if (mlcl_elastic._instance) {
            throw new Error("Error: Instantiation failed. Singleton module! Use .getInstance() instead of new.");
        }
        mlcl_elastic._instance = this;
        mlcl_elastic.molecuel.on('mlcl::core::init:post', (molecuel) => {
            this.config = molecuel.config.search;
        });
        mlcl_elastic.molecuel.on('mlcl::queue::init:post', (queue) => {
            this.queue = queue;
            this.connect((err, connection) => {
                if (err) {
                    mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while connecting' + err);
                }
                else {
                    this.connection = connection;
                    mlcl_elastic.molecuel.emit('mlcl::search::connection:success', this);
                }
            });
        });
        mlcl_elastic.molecuel.on('mlcl::database::registerModel:pre', (database, modelname, schema, options) => {
            if (options.indexable) {
                options.modelname = modelname;
                schema.plugin(this.plugin, options);
                mlcl_elastic.molecuel.emit('mlcl::elastic::registerPlugin:post', this, modelname, schema);
            }
        });
        mlcl_elastic.molecuel.on('mlcl::database::registerModel:post', (database, modelname, model) => __awaiter(this, void 0, void 0, function* () {
            mongolastic.registerModel(model, (err) => __awaiter(this, void 0, void 0, function* () {
                if (err) {
                    mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while registering model to elasticsearch' + err);
                }
                else {
                    if (mlcl_elastic.molecuel.serverroles && mlcl_elastic.molecuel.serverroles.worker) {
                        var qname = 'mlcl__elastic__' + modelname + '_resync';
                        this.queue.ensureQueue(qname, (err) => {
                            if (!err) {
                                this.queue.client.createReceiver(qname).then((receiver) => {
                                    receiver.on('message', (msg) => {
                                        var id = msg.body.toString();
                                        if (id) {
                                            model.syncById(id, (err) => __awaiter(this, void 0, void 0, function* () {
                                                if (!err) {
                                                    receiver.accept(msg);
                                                }
                                                else {
                                                    mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
                                                    receiver.release(msg);
                                                }
                                            }));
                                        }
                                    });
                                }).error((qerr) => {
                                    mlcl_elastic.molecuel.log.error('mlcl_elastic', qerr);
                                });
                            }
                            else {
                                mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
                            }
                        });
                    }
                }
            }));
        }));
    }
    static getInstance() {
        if (mlcl_elastic._instance === null) {
            mlcl_elastic._instance = new mlcl_elastic();
        }
        return mlcl_elastic._instance;
    }
    static init(m) {
        mlcl_elastic.molecuel = m;
        return mlcl_elastic.getInstance();
    }
    connect(callback) {
        mongolastic.connect(this.config.prefix, this.config, callback);
    }
    ensureIndex(modelname, callback) {
        mongolastic.indices.exists((modelname, err, exists) => {
            if (!exists) {
                var mappings = {};
                mappings[modelname] = {
                    properties: {
                        url: {
                            type: 'keyword',
                            index: true
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
                mongolastic.indices.create(modelname, settings, mappings, (err) => {
                    if (err) {
                        mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while creating indices' + err);
                    }
                });
                callback();
            }
            else {
                callback();
            }
        });
    }
    index(modelname, entry, callback) {
        mongolastic.index(modelname, entry, callback);
    }
    delete(modelname, entry, callback) {
        mongolastic.delete(modelname, entry, callback);
    }
    deleteIndex(modelname, callback) {
        mongolastic.deleteIndex(modelname, callback);
    }
    sync(model, modelname, callback) {
        mongolastic.sync(model, modelname, callback);
    }
    resync(modelname, query) {
        var elast = mlcl_elastic.getInstance();
        var dbmodel = this;
        if (modelname) {
            const queuename = 'mlcl__elastic_resync';
            this.queue.ensureQueue(queuename, (err) => {
                elast.queue.client.createSender(queuename).then((sender) => {
                    var count = 0;
                    var stream = dbmodel.find(query, '_id').lean().stream();
                    stream.on('error', function (err) {
                        mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
                    });
                    stream.on('data', (obj) => {
                        count++;
                        sender.send({ id: obj._id.toString(), model: modelname });
                    });
                    stream.on('end', function () {
                        mlcl_elastic.molecuel.log.info('mlcl_elastic', 'reindex for ' + modelname + ' has been added to queue, ' + count + 'items');
                    });
                }).error((err) => {
                    mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
                });
            });
        }
    }
    search(query, callback) {
        var elast = mlcl_elastic.getInstance();
        if (query && query.index) {
            var i = query.index.split(',');
            query.index = i.map(function (index) {
                return elast.getIndexName(index);
            }).join(',');
        }
        mongolastic.search(query, callback);
    }
    searchByUrl(url, lang, callback) {
        mlcl_elastic.getInstance().search({
            body: {
                from: 0,
                size: 1,
                query: {
                    bool: {
                        filter: {
                            term: {
                                url: url
                            }
                        },
                        should: [
                            {
                                term: {
                                    lang: lang
                                }
                            },
                            {
                                bool: {
                                    must_not: {
                                        exists: {
                                            field: 'lang'
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }, callback);
    }
    searchById(id, callback) {
        mlcl_elastic.getInstance().search({
            body: {
                query: {
                    term: {
                        _id: id
                    }
                }
            }
        }, callback);
    }
    exists(indexname, callback) {
        this.connection.indices.exists({
            index: this.getIndexName(indexname),
        }, callback);
    }
    create(indexname, settings, mappings, callback) {
        var elast = mlcl_elastic.getInstance();
        elast.connection.indices.create({
            index: elast.getIndexName(indexname),
            body: {
                settings: settings,
                mappings: mappings
            }
        }, callback);
    }
    checkCreateIndex(indexname, settings, mappings, callback) {
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
    }
    getMapping(indexname, callback) {
        var elast = mlcl_elastic.getInstance();
        elast.connection.indices.getMapping({
            index: elast.getIndexName(indexname)
        }, callback);
    }
    plugin(schema, options) {
        schema.plugin(mongolastic.plugin, options);
        var mylastic = mlcl_elastic.getInstance();
        schema.statics.searchByUrl = mylastic.searchByUrl;
        schema.statics.searchById = mylastic.searchById;
        schema.statics.resync = mylastic.resync;
        schema.methods.searchByUrl = mylastic.searchByUrl;
        schema.methods.searchById = mylastic.searchById;
    }
    getIndexName(name) {
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
    }
}
mlcl_elastic._instance = null;
module.exports = mlcl_elastic.init;
