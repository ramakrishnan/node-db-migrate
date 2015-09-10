var util = require('util');
var cassandra = require('cassandra-driver');
var Base = require('./base');
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

  createTable: function(tableName, options, constraints, callback) {
    if (typeof constraints == 'function') {
      var callback = constraints;
      var constraints = {};
    }
    var self = this;
    var cql = [];
    var metaData = [];

    var columns = Object.keys(options);

    cql.push(util.format('CREATE TABLE IF NOT EXISTS %s', tableName));
    cql.push('(');
    columns.forEach(function(column) {
      metaData.push(column + ' ' + options[column]);
    });
    var primaryKey = constraints['primary_key'];
    if (!/^\(.*\)$/.test(primaryKey)) {
      primaryKey = '(' + primaryKey + ')';
    }
    metaData.push('PRIMARY KEY ' + primaryKey);
    cql.push(metaData.join(', '));
    cql.push(')');
    var cqlString = cql.join(' ');
    return this.runSql(cqlString, callback).nodeify(callback).nodeify(callback);
  },

  dropTable: function(tableName, callback) {
    var cqlString = util.format('DROP TABLE %s', tableName);
    return this.runSql(cqlString, callback).nodeify(callback);
  },

  allLoadedMigrations: function(callback) {
    var cqlString = 'SELECT * from migrations';
    return this.runSql(cqlString)
      .then(function(data) {
        var sortedData = data.rows.map(function(item) {
          item.moment_time = moment(item.ran_on).valueOf();
          return item
        });
        // Order migration records in ascending order.
        return sortedData.sort(function(x,y) {
          if (x.moment_time > y.moment_time) return -1;
          if (x.moment_time < y.moment_time) return 1;
          return 0;
        })
      })
      .nodeify(callback);
  },

  addMigrationRecord: function (name, callback) {
    var formattedDate = name.split('-')[0].replace('/', '');
    formattedDate = moment(formattedDate, 'YYYY-MM-DD HH:mm:ss');
    formattedDate = moment(formattedDate).format('YYYY-MM-DD HH:mm:ss');
    var command = util.format('INSERT INTO %s  (name, ran_on) VALUES (\'%s\', \'%s\')', internals.migrationTable, name, formattedDate);
    return this.runSql(command).nodeify(callback);
  },

  /**
   * Deletes a migration
   *
   * @param migrationName  - The name of the migration to be deleted
   * @param callback
   */
  deleteMigration: function(migrationName, callback) {
    var command = util.format('DELETE FROM %s where name = \'/%s\'', internals.migrationTable, migrationName);
    return this.runSql(command).nodeify(callback);
  },

  close: function() {
    this.connection.shutdown();
  },

  createMigrationsTable: function(callback) {
    var tableOptions = {
      'name': 'varchar',
      'ran_on': 'timestamp'
    };
    var constraints = {
      'primary_key': 'name'
    };
    return this.createTable(internals.migrationTable, tableOptions, constraints).nodeify(callback);
  },

  /**
   * Function to execute the CQL statement through cassandra-driver execute method.
   *
   * @param string
   * @param Object
   */

  runSql: function(command, callback) {
    var self = this;
    return new Promise(function(resolve, reject) {
        var prCB = function(err, data) {
          if (err) {
            log.error(err.message);
            log.info(command);
          }
          return (err ? reject('err') : resolve(data));
        };
        self.connection.execute(command, function(err, result) {
          prCB(err, result)
        });
    });
  }

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
