var util = require('util');
var cassandra = require('cassandra-driver');
var semver = require('semver');
var Base = require('./base');
var type = require('../data_type');
var log = require('../log');
var Promise = require('bluebird');
var moment = require('moment');

var connectionString, internals = {};

var CqlshDriver = Base.extend({

  init: function(connection, params) {
    this._super(internals);
    this.connection = connection;
    this.connectionParam = params;
  },

  createTable: function(tableName, options, callback) {
    // var cqlString = this._getCreateTableString(tableName, options);
    var cqlString = 'CREATE TABLE  IF NOT EXISTS ' + tableName + ' (name varchar primary key, run_on varchar)';
    return this._executeCql(cqlString, callback)
      .bind(this)
      .nodeify(callback);
  },

  allLoadedMigrations: function(callback) {
    var cqlString = 'SELECT * from migrations;';
    return this._executeCql(cqlString)
      .then(function(data) {
        return data.rows;
      })
      .bind(this)
      .nodeify(callback);
  },

  addMigrationRecord: function (name, callback) {
    var formattedDate = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
    var command = util.format('INSERT INTO %s  (name, run_on) VALUES (\'%s\', \'%s\')', internals.migrationTable, name, formattedDate);
    return this._executeCql(command).nodeify(callback);
  },

  close: function() {
    this.connection.shutdown();
  },

  createMigrationsTable: function(callback) {
    var self = this;
    return self._createMigrationsTable(callback)
      .bind(this)
      .nodeify(callback);
  },

  _getCreateTableString: function(tableName, options) {
    return sql = util.format('CREATE TABLE `%s`', tableName);
  },

  _executeCql: function(command, callback) {
    var self = this;
    util.debug(command);
    return new Promise(function(resolve, reject) {
        var prCB = function(err, data) {
          return (err ? reject(err) : resolve(data));
        };
        self.connection.execute(command, function(err, result) {
          prCB(err, result)   
        });
    }).nodeify(callback);
  },

  /**
   * Creates the migrations table
   *
   * @param callback
   */
  _createMigrationsTable: function(callback) {
    var tableOptions = {
      'name': {type: 'varchar', primaryKey: true},
      'ran_on': 'varchar'
    }
    return this.createTable(internals.migrationTable, tableOptions).nodeify(callback);
  },

});

Promise.promisifyAll(CqlshDriver);
/**
 * Gets a connection to cassandra
 *
 * @param config    - The config to connect to mongo
 * @param callback  - The callback to call with a Cassandra object
 */
exports.connect = function(config, intern, callback) {
  var db;
  var port;
  var host;

  internals = intern;

  // Make sure the keyspace is defined
  if(config.database === undefined) {
    throw new Error('keyspace must be defined in database.json');
  }

  if(config.host === undefined) {
    host = 'localhost';
  } else {
    host = config.host;
  }

  if(config.port === undefined) {
    port = 9160;
  } else {
    port = config.port;
  }

  // TODO: See if we need connectionParam or can directly be driven from cofig
  var connectionParam = {}
  if(config.user !== undefined && config.password !== undefined) {
    connectionParam.user = config.user,
    connectionParam.password = config.password
  }
  connectionParam.hostname = config.host;
  connectionParam.keyspace = config.database;

  db = config.db || new cassandra.Client({ 
    contactPoints: connectionParam.hostname, 
    keyspace: connectionParam.keyspace
  });

  callback(null, new CqlshDriver(db, connectionParam));
};
