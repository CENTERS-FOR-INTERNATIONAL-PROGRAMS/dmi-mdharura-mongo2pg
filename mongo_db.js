const MongoClient = require("mongodb").MongoClient;

const config = require("./db_details.js").config;

// MongoDB database connection module

// -------------------------------------

let url = config.mongo_connection.url;

let _db;

module.exports = {
  connectToServer: function (callback) {
    MongoClient.connect(url, { useNewUrlParser: true }, function (err, client) {
      _db = client.db(config.mongo_connection.database);
      return callback(err);
    });
  },

  getDb: function () {
    return _db;
  },
};
