/***
 * Molecuel CMS - Elasticsearch integration module
 * @type {exports}
 */
var mongolastic = require('mongolastic');
var molecuel;

var elastic = function elastic() {
  this._registerEvents();
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
      schema.plugin(mongolastic.plugin, {modelname: modelname});
      molecuel.emit('mlcl::elastic::registerPlugin:post', self, modelname, schema);
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
 * Singleton getInstance definition
 * @return singleton class
 */
elastic.getInstance = function(){
  if(this.instance === null){
    this.instance = new elastic();
  }
  return this.instance;
};

function init(m) {
  molecuel = m;
  return elastic.getInstance();
}

module.exports = init;