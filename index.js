/***
 * Molecuel CMS - Elasticsearch integration module
 * @type {exports}
 */
var mongolastic = require('mongolastic');
var molecuel;

var elastic = function elastic() {
  this._registerEvents();
};

/////////////////////
// singleton stuff
////////////////////
var instance = null;

var getInstance = function(){
  return instance || (instance = new elastic());
};

elastic.prototype._registerEvents = function _registerEvents() {
  var self = this;
  // register on init function of core and create connection
  molecuel.on('mlcl::core::init:post', function(molecuel) {
    self.config = molecuel.config.search;
    self.connect(function(err, connection) {
      if(err) {
        console.log(err);
      } else {
        self.connection = connection;
        molecuel.emit('mlcl::search::connection:success', self);
      }
    });
  });

  // Schema creation event
  // this is used to register the plugin to the schema
  molecuel.on('mlcl::database::registerModel:pre', function(database, modelname, schema, options) {
    if(options.indexable) {
      self.ensureIndex(modelname, function(err, result) {
        schema.plugin(self.plugin, {modelname: modelname});
        molecuel.emit('mlcl::elastic::registerPlugin:post', self, modelname, schema);
      });
    }
  });
};

/* ************************************************************************
 SINGLETON CLASS DEFINITION
 ************************************************************************ */
elastic.instance = null;

/**
 * Connect the instance to elastic
 * @param callback
 */
elastic.prototype.connect = function connect(callback) {
  mongolastic.connect(this.config.prefix, this.config.options, callback);
};

/**
 * Checks if the index for given modelname exists and if not creates it with "default" mapping settings
 *
 * @param modelname
 * @param callback
 * @todo make settings/mappings configurable
 */
elastic.prototype.ensureIndex = function ensureIndex(modelname, callback) {
  //check if index already exists
  mongolastic.indices.exists(modelname, function(err, exists) {
    if(!exists) {
      var mappings = {};  //@todo: get specific mapping information from model definition or cenral config?
      mappings[modelname] = {
        properties: {
          url: {
            type: 'string', index: 'not_analyzed' // by default url information must be not_analyzed,
          }                                                  // maybe we should go with this solution:
        // http://joelabrahamsson.com/elasticsearch-101/"type": "multi_field",
          /*url: {
            "fields": {
              "url": {"type": "string"},
              "original": {"type" : "string", "index" : "not_analyzed"}
            }
          }*/
        }
      }
      var settings = {}; //@todo: make settings configurable from model definition or central config?
      mongolastic.indices.create(modelname, settings, mappings, function(err, res) {
        if(err) {
          console.log(err);
        } else {
          console.log(res);
        }
      });
      callback();
    } else {
      callback();
    }
  });
}
/**
 * Index the object
 * @param modelname
 * @param entry
 * @param callback
 */
elastic.prototype.index = function index(modelname, entry, callback) {
  mongolastic.index(modelname, entry, callback);
};

/**
 * Delete function for objects
 * @param modelname
 * @param entry
 * @param callback
 */
elastic.prototype.delete = function(modelname, entry, callback) {
  mongolastic.delete(modelname, entry, callback);
};

/**
 * Delete Index from elasticsearch
 * @param callback
 */
elastic.prototype.deleteIndex = function connect(modelname, callback) {
  mongolastic.deleteIndex(modelname, callback);
};

/**
 * Sync function for data model
 * @param model
 * @param modelname
 * @param callback
 */
elastic.prototype.sync = function sync(model, modelname, callback) {
  mongolastic.sync(model, modelname, callback);
};

/**
 * Search for objects in elasticsearch
 * @param query
 * @param callback
 */
elastic.prototype.search = function search(query, callback) {
  mongolastic.search(query, callback);
};

/**
 * Gets a entry by url and language
 * @param url
 * @param lang
 */
elastic.prototype.searchByUrl = function searchByUrl(url, lang, callback) {
  getInstance().search({
    body: {
      query: {
        filtered : {
          query : {
            query_string: {
              default_field : 'url',
              query : url.replace(/\//g, '\\/'),
              phrase_slop: 0,
              default_operator: 'AND'
            }
          },
          filter : {
            or: [
              {
                term : {
                  lang : lang
                }
              },
              {
                missing: {
                  field: 'lang'
                }
              }
            ]
          }
        }
      }
    }
  }, callback);
};
/**
 * Get a object by id
 * @param id
 * @param callback
 */
elastic.prototype.searchById = function searchById(id, callback) {
  this.search({
    body: {
      query: {
        filtered : {
          filter : {
            term : {
              _id : id
            }
          }
        }
      }
    }
  }, callback);
};

/**
 * Extended mongoose plugin
 * @param schema
 * @param options
 */
elastic.prototype.plugin = function plugin(schema, options) {
  schema.plugin(mongolastic.plugin, options);

  var mylastic = getInstance();

  schema.statics.searchByUrl = mylastic.searchByUrl;
  schema.statics.searchById = mylastic.searchById;

  schema.methods.searchByUrl = mylastic.searchByUrl;
  schema.methods.searchById = mylastic.searchById;
};

function init(m) {
  molecuel = m;
  return getInstance();
}

module.exports = init;