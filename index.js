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
      //self.ensureIndex(modelname, function() {});
      schema.plugin(self.plugin, {modelname: modelname});
      molecuel.emit('mlcl::elastic::registerPlugin:post', self, modelname, schema);
    }
  });

  molecuel.on('mlcl::database::registerModel:post', function(database, modelname, model) {
    // returns err and model
    mongolastic.registerModel(model, function() {
      //@todo Error log
    });
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
  var elast = getInstance();
  mongolastic.connect(elast.config.prefix, elast.config, callback);
};

elastic.prototype.indexNameFromModel = function(modelname) {
  return this.config.prefix + '-' + modelname.toLowerCase();
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
      //@todo: get specific mapping information from model definition or central config?
      var mappings = {};
      mappings[modelname] = {
        properties: {
          url: {
            type: 'string',
            index: 'not_analyzed' // by default url information must be not_analyzed,
          },
          'location': {
            'properties': {
              geo: {
                type: 'geo_point',
                'lat_lon': true
              }
            }
          }
          /*}*/
          // maybe we should go with this solution:
          // http://joelabrahamsson.com/elasticsearch-101/"type": "multi_field",
          /*url: {
            "fields": {
              "url": {"type": "string"},
              "original": {"type" : "string", "index" : "not_analyzed"}
            }
          }*/
        }
      };
      var settings = {}; //@todo: make settings configurable from model definition or central config?
      mongolastic.indices.create(modelname, settings, mappings, function(err) {
        if(err) {
          console.log(err);
        }
      });
      callback();
    } else {
      callback();
    }
  });
};
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
  var elast = getInstance();
  if(query && query.index) {
    query.index = elast.getIndexName(query.index);
  }
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
      from: 0,
      size: 1,
      filter : {
        and: [
          {
            term: {
              url: url
            }
          },
          {
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
        ]
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
 * Return a boolean indicating whether index exists
 *
 * @param indexname
 * @param callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-exists
 */
elastic.prototype.exists = function(indexname, callback) {
  var elast = getInstance();
  elast.connection.indices.exists({
    index: elast.getIndexName(indexname),
  }, callback);
};


/**
 * Create an index in Elasticsearch.
 *
 * @param indexname
 * @param settings
 * @param mappings
 * @param callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-create
 */
elastic.prototype.create = function(indexname, settings, mappings, callback) {
  var elast = getInstance();
  elast.connection.indices.create({
    index: elast.getIndexName(indexname),
    body: {
      settings: settings,
      mappings: mappings
    }
  }, callback);
};

/**
 * Check if the index exists and create a new one
 * @param model
 * @param callback
 */
elastic.prototype.checkCreateIndex = function checkCreateIndex(indexname, settings, mappings, callback) {
  var elast = getInstance();
  elast.exists(indexname, function(err, response) {
    if(!response) {
      elast.create(indexname, settings, mappings, function (err) {
        callback(err, true);
      });
    } else {
      callback(err, false);
    }
  });
};

/**
 * Get the mapping
 * @param
 */
elastic.prototype.getMapping = function getMapping(indexname, callback) {
  var elast = getInstance();
  elast.connection.indices.getMapping({
    index: elast.getIndexName(indexname)
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

/**
 * Helper for hamornising namespaces
 * @param modelname
 * @returns {string}
 */
elastic.prototype.getIndexName = function(name) {
  var elast = getInstance();
  if(elast.config.prefix) {
    return elast.config.prefix + '-' + name.toLowerCase();
  } else {
    return name.toLowerCase();
  }
};

function init(m) {
  molecuel = m;
  return getInstance();
}

module.exports = init;
