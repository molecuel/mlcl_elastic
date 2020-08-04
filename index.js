"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
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
                    if (!mlcl_elastic.models.has(modelname)) {
                        mlcl_elastic.models.set(modelname, model);
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
        var dataCache = [];
        var batchSize = Math.floor(elast.config.resyncBatchSize / 2) || 20;
        var count = 0;
        if (modelname) {
            var stream = dbmodel.find(query, '_id').lean().stream();
            stream.on('error', function (err) {
                mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
            });
            stream.on('data', (obj) => {
                dataCache = dataCache.concat(obj);
            });
            stream.on('end', function () {
                mlcl_elastic.molecuel.log.info('mlcl_elastic', 'reindex for ' + modelname + ' is running, ' + dataCache.length + ' items.');
                const chunkedData = ((arr, chunkSize) => {
                    var R = [];
                    for (var i = 0, len = arr.length; i < len; i += chunkSize)
                        R.push(arr.slice(i, i + chunkSize));
                    return R;
                })(dataCache, batchSize);
                var timeout = 0;
                for (const chunk of chunkedData) {
                    timeout += 0.5e3;
                    setTimeout(() => {
                        Promise.all(chunk.map(obj => new Promise(resolve => {
                            mongolastic.syncById(mlcl_elastic.models.get(modelname), modelname, obj._id.toString(), (err) => {
                                if (err) {
                                    mlcl_elastic.molecuel.log.error('mlcl_elastic', err);
                                    resolve(0);
                                }
                                else {
                                    resolve(1);
                                }
                            });
                        }))).then((vals) => count += vals.reduce((a, b) => a + b, 0));
                    }, timeout);
                }
                setTimeout(() => {
                    mlcl_elastic.molecuel.log.info('mlcl_elastic', 'reindexed ' + count + ' ' + modelname + 's;');
                }, timeout + 1e3);
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
mlcl_elastic.models = new Map();
function init(m) {
    return mlcl_elastic.init(m);
}
;
module.exports = init;
