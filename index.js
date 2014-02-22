"use strict";
var util = require('util'),
    fs = require('fs'),
    mongodb = require('mongodb'),
    Q = require('q');

function MongoBackend(config) {
    var self = this;
    var database;
    var connect = Q.defer();
    var cache = Q.defer();
    var collections = { };

    var getCollection = function (col) {
        collections[col] = collections[col] || function (database) {
            var prom = Q.defer();
            database.collection(col, function (err, collection) {
                if (err) {
                    prom.reject(err);
                } else {
                    prom.resolve(collection);
                }
            });
            return prom.promise;
        }
        return collections[col];
    };

    this.connect = function () {
        if (!self.connected) {
            mongodb.MongoClient.connect('mongodb://' + (config.backendconf.mongo.hostname || '127.0.0.1') + '/' + self.namespace, { auto_reconnect: true, readPreference: 'nearest' }, function (err, db) {
                if (err) connect.reject(err);
                database = db;
                connect.resolve(database);
                self.connected = true;
            });
        }
        return connect.promise;
    }

    this.wait = function (callback) {
        if (callback) {
            self.connect().done(callback);
        } else {
            return self.connect();
        }
    };

    this.database = function () {
        return database;
    };

    this.get = function (col, keys, callback) {
        self.connect().then(getCollection(col)).done(function (collection) {
            var query = { };
            if (keys !== '*') {
                query = util.isArray(keys) ? { _id: { $in: keys } } : { _id: keys };
            }
            collection.find(query).toArray(function (err, recs) {
                if (keys === '*' || util.isArray(keys)) {
                    var results = { };
                    recs.forEach(function (rec) {
                        results[rec._id] = JSON.parse(rec.value);
                    });
                    callback(err, results);
                } else if (recs[0] && recs[0].value) {
                    callback(err, JSON.parse(recs[0].value));
                } else if (recs[0]) {
                    callback(err, recs[0]);
                } else {
                    callback(err, null);
                }
            });
        }, function (err) { callback(err, null); });
    }; 

    this.set = function (col, key, object, callback, options) {
        options = options || { };
        self.connect().then(getCollection(col)).done(function (collection) {
            var obj;
            if (options.raw) {
                obj = object;
            } else {
                obj = { _id: key, value: JSON.stringify(object) };
            }
            if (options.expiration) {
                var expire = new Date();
                expire.setSeconds(expire.getSeconds() + options.expiration);
                obj.expire = expire;
            }
            collection.save(obj, function (err, result) {
                if (typeof callback === 'function') callback(err, result);
            });
        }, function(err) { callback(err, null); });
    };

    this.del = function (col, key, callback) {
        self.connect().then(getCollection(col)).done(function (collection) {
            collection.remove({ _id: key }, function (err, result) {
                if (typeof callback === 'function') callback(err, result);
            });
        }, function(err) { callback(err, null); });
    };

    self.cache = {
        init: function () {
            var prom = Q.defer();
            self.connect().then(getCollection('cache')).done(function (collection) {
                collection.ensureIndex({ "expire": 1}, { expireAfterSeconds: 0 }, function (err, index) {
                    if (err) {
                        prom.reject(err);
                    } else {
                        prom.resolve(index);
                    }
                });
            });
            return prom.promise;
        },
        get: function (keys, callback) {
            self.cache.init().done(function() {
                self.get('cache', keys, callback);
            }, function(err) { callback(err, null); });
        },
        set: function (key, value, expiration, callback) {
            self.cache.init().done(function() {
                self.set('cache', key, value, callback, { expiration: expiration });
            }, function(err) { callback(err, null); });
        }
    };

    self.media = {
        send: function (recordid, name, res) {
            self.connect().done(function () {
                var gs = new mongodb.GridStore(database, recordid + '/' + name, 'r');
                gs.open(function (err, gs) {
                    if (err) {
                        res.send(404);
                    } else {
                        gs.stream(true);
                        gs.pipe(res);
                    }
                });
            });
        },
        save: function (recordid, name, metadata, tmppath, callback) {
            self.connect().done(function () {
                var gs = new mongodb.GridStore(database, recordid + '/' + name, 'w', metadata);
                gs.open(function(err, gridStore) {
                    gridStore.writeFile(tmppath, function (err) {
                        fs.unlink(tmppath, function () {
                            callback(err);
                        });
                    });
                });
            });
        },
        del: function (recordid, name, callback) {
            self.connect().then(function () {
                mongodb.GridStore.unlink(database, recordid + '/' + name, callback);
            });
        }
    };

    self.dump = function (outstream, promise, wantedList) {
        self.connect().done(function () {
            database.collections(function (err, collections) {
                var wanted = { };
                var queue = [ ];
                if (wantedList) {
                    wantedList.forEach(function (col) {
                        wanted[col] = true;
                    });
                }
                var dumpCollection = function (col, repeat) {
                    if (repeat) outstream.write(",\n");
                    outstream.write('"' + col.collectionName + '": [');
                    var colstream = col.find().stream();
                    var firstChunk = true;
                    colstream.on('data', function (chunk) {
                        if (!firstChunk) outstream.write(",");
                        firstChunk = false;
                        outstream.write(JSON.stringify(chunk));
                    });
                    colstream.on('end', function () {
                        outstream.write("]");
                        if (queue.length > 0) {
                            dumpCollection(queue.shift(), true);
                        } else {
                            outstream.end("}\n", function () {
                                promise.resolve(true);
                            });
                        }
                    });
                };
                collections.forEach(function (col, index) {
                    if ((!wantedList || wanted[col.collectionName]) && col.collectionName !== 'system.indexes') {
                        queue.push(col);
                    }
                });
                if (queue.length > 0) {
                    outstream.write("{\n");
                    dumpCollection(queue.shift());
                } else {
                    promise.resolve(true);
                }
            });
        });
    };

    config.backendconf = config.backendconf || { };
    config.backendconf.mongo = config.backendconf.mongo || { };
    self.namespace = config.backendconf.mongo.namespace || 'biblionarrator';
    self.cacheexpire = config.cacheconf.defaultexpiry || 600;
    self.connected = false;
}

module.exports = MongoBackend;
module.exports.description = 'MongoDB backend (clusterable)';
module.exports.features = {
    datastore: true,
    mediastore: true,
    cache: true
};
