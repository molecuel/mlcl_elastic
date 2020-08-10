import mongolastic = require('mongolastic');
import async = require('async');
import { promisify } from 'util';

class mlcl_elastic {

  private static _instance: mlcl_elastic = null;
  public static molecuel;
  public static models = new Map<string, any>();
  public config: any;
  public connection: any;
  // @todo: Add mlcl_queue type here
  public queue: any;

  constructor() {
    if (mlcl_elastic._instance) {
      throw new Error("Error: Instantiation failed. Singleton module! Use .getInstance() instead of new.");
    }

    mlcl_elastic._instance = this;

    // register on init function of core and create connection
    mlcl_elastic.molecuel.on('mlcl::core::init:post', (molecuel) => {
      this.config = molecuel.config.search;
      this.connect((err, connection) => {
        if (err) {
          mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while connecting' + err);
        } else {
          this.connection = connection;
          mlcl_elastic.molecuel.emit('mlcl::search::connection:success', this);
        }
      });
    });


    // Schema creation event
    // this is used to register the plugin to the schema
    mlcl_elastic.molecuel.on('mlcl::database::registerModel:pre', (database, modelname, schema, options) => {
      if (options.indexable) {
        //self.ensureIndex(modelname, function() {});
        options.modelname = modelname;
        schema.plugin(this.plugin, options);
        mlcl_elastic.molecuel.emit('mlcl::elastic::registerPlugin:post', this, modelname, schema);
      }
    });

    mlcl_elastic.molecuel.on('mlcl::database::registerModel:post', async (database, modelname, model) => {
      // returns err and model
      mongolastic.registerModel(model, async (err) => {
        if (err) {
          mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while registering model to elasticsearch' + err);
        } else {
          if (!mlcl_elastic.models.has(modelname)) {
            mlcl_elastic.models.set(modelname, model);
          }
        }
      });
    });
  }

  public static getInstance(): mlcl_elastic {
    if (mlcl_elastic._instance === null) {
      mlcl_elastic._instance = new mlcl_elastic();
    }
    return mlcl_elastic._instance;
  }

  public static init(m): mlcl_elastic {
    mlcl_elastic.molecuel = m;
    return mlcl_elastic.getInstance();
  }

  /**
   * Connect the instance to elastic
   * @param callback
   */
  public connect(callback: Function): void {
    mongolastic.connect(this.config.prefix, this.config, callback);
  }

  public ensureIndex(modelname: string, callback: Function): void {
    //check if index already exists
    mongolastic.indices.exists((modelname, err, exists: boolean) => {
      if (!exists) {
        //@todo: get specific mapping information from model definition or central config?
        var mappings: { [index: string]: any; } = {};
        mappings[modelname] = {
          properties: {
            url: {
              type: 'keyword',
              index: true
            },
            'location': {
              'properties': {
                geo: {
                  type: 'geo_point'
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
        mongolastic.indices.create(modelname, settings, mappings, (err) => {
          if (err) {
            mlcl_elastic.molecuel.log.error('mlcl_elastic', 'Error while creating indices' + err);
          }
        });
        callback();
      } else {
        callback();
      }
    });
  }

  public index(modelname: String, entry: any, callback: Function): void {
    mongolastic.index(modelname, entry, callback);
  }

  /**
   * Delete Index from elasticsearch
   * @param callback
   */
  public delete(modelname: String, entry: any, callback: Function): void {
    mongolastic.delete(modelname, entry, callback);
  }

  /**
   * Delete Index from elasticsearch
   * @param callback
   */
  public deleteIndex(modelname: string, callback: Function) {
    mongolastic.deleteIndex(modelname, callback);
  }

  /**
   * Sync function for data model
   * @param model
   * @param modelname
   * @param callback
   */
  public sync(model: any, modelname: string, callback: Function) {
    mongolastic.sync(model, modelname, callback);
  }

  /**
   * Search for objects in elasticsearch
   * @param query
   * @param callback
   */
  public search(query: any, callback: Function): void {
    var elast = mlcl_elastic.getInstance();
    if (query && query.index) {
      //rewrite index
      var i = query.index.split(',');
      query.index = i.map(function (index) {
        return elast.getIndexName(index);
      }).join(',');
    }
    mongolastic.search(query, callback);
  }

  /**
   * Gets a entry by url and language
   * @param url
   * @param lang
   */
  public searchByUrl(url: String, lang: String, callback: Function): void {
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

  /**
   * Get a object by id
   * @param id
   * @param callback
   */
  public searchById(id: Number, callback: Function): void {
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


  /**
   * Return a boolean indicating whether index exists
   *
   * @param indexname
   * @param callback
   * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-exists
   */
  public exists(indexname: string, callback: Function): void {
    this.connection.indices.exists({
      index: this.getIndexName(indexname),
    }, callback);
  }


  /**
   * Create an index in Elasticsearch.
   *
   * @param indexname
   * @param settings
   * @param mappings
   * @param callback
   * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-create
   */
  public create(indexname, settings, mappings, callback) {
    var elast = mlcl_elastic.getInstance();
    elast.connection.indices.create({
      index: elast.getIndexName(indexname),
      body: {
        settings: settings,
        mappings: mappings
      }
    }, callback);
  }


  /**
   * Check if the index exists and create a new one
   * @param model
   * @param callback
   */
  public checkCreateIndex(indexname, settings, mappings, callback) {
    var elast = mlcl_elastic.getInstance();
    elast.exists(indexname, function (err, response) {
      if (!response) {
        elast.create(indexname, settings, mappings, function (err) {
          callback(err, true);
        });
      } else {
        callback(err, false);
      }
    });
  }

  /**
   * Get the mapping
   * @param
   */
  public getMapping(indexname, callback) {
    var elast = mlcl_elastic.getInstance();
    elast.connection.indices.getMapping({
      index: elast.getIndexName(indexname)
    }, callback);
  }

  /**
   * Extended mongoose plugin
   * @param schema
   * @param options
   */
  public plugin(schema, options) {
    schema.plugin(mongolastic.plugin, options);

    var mylastic = mlcl_elastic.getInstance();

    schema.statics.searchByUrl = mylastic.searchByUrl;
    schema.statics.searchById = mylastic.searchById;

    schema.methods.searchByUrl = mylastic.searchByUrl;
    schema.methods.searchById = mylastic.searchById;
  }

  /**
   * Helper for hamornising namespaces
   * @param modelname
   * @returns {string}
   */
  public getIndexName(name: string) {
    var elast = mlcl_elastic.getInstance();
    if (elast.config.prefix) {
      if (name.indexOf(elast.config.prefix + '-') === 0) {
        return name.toLowerCase();
      } else {
        return elast.config.prefix + '-' + name.toLowerCase();
      }
    } else {
      return name.toLowerCase();
    }
  }
}

function init(m) {
  return mlcl_elastic.init(m);
};

export = init;
