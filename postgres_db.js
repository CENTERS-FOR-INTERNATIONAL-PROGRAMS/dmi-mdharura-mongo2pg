const pg = require("pg");

const config = require("./db_details.js").config;

// Postgres database connection module

// -------------------------------------

let pg_config = config.postgres_connection;

let pg_connect = new pg.Pool(pg_config);

module.exports = () => {
  return pg_connect;
};
