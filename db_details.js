require("dotenv").config({ path: __dirname + "/.env" });

module.exports.config = {
  mongo_connection: {
    url: process.env.MONGO_DB_URL || "mongodb://localhost:27017",
    database: process.env.MONGO_DB_NAME,
  },
  postgres_connection: {
    user: process.env.PG_DB_USER || "postgres",
    database: process.env.PG_DB_NAME,
    host: process.env.PG_DB_HOST || "localhost",
    password: process.env.PG_DB_PASSWORD,
    port: process.env.PG_DB_PORT || "5432",
  },
};
