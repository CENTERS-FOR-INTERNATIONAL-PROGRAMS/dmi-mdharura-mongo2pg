let async = require("async");
let moment = require("moment");
let waterfall = require("async-waterfall");
let report_models = require("./data_model.js");
let MongoConnection = require("./mongo_db.js");
var PostgresConnection = require("./postgres_db.js");

let db;

// Data migration method and module. Logic for migrating data from MongoDB to PostgreSQL

// -------------------------------------

let MigrationIsRunning = false;

module.exports = function (complete) {
  if (MigrationIsRunning) {
    console.log("Migration script is still running");
    MigrationIsRunning = true;
    return;
  }
  MigrationIsRunning = true;
  console.log("start");

  // Extract  all models from data_model.js
  let models = Object.getOwnPropertyNames(report_models.all_models);
  let now = moment().format("");
  let models_index = [];

  for (let rm in models) {
    models_index.push(rm);
  }

  // Declared variables
  let table_name = "";
  let columns = [];
  let latest_update_date = "";
  let latest_create_date = "";
  let columns_string = "";
  let pg_columns_String = "";
  let pgUpdateStatement = "";
  let pgInsertStatement = "";
  let no_data_flag = "";
  let pg_columns = [];
  let updated_at_flag = false;
  let created_at_flag = false;
  let model_name = "";
  let model_data_type = [];
  let pg_update_res;
  let pg_create_res;

  let model_transform = async function (base_model, cb) {
    // Get table name
    let base_model_input = report_models.all_models[models[base_model]];
    let model_input = base_model_input.slice();
    model_data_type = model_input.map(function (x) {
      return x.split(/\s+/)[1];
    });

    table_name = model_input.shift();
    model_name = table_name.replace(/s$/, "");
    model_data_type.shift();

    // remove data type information from model
    columns = [];
    let index = 0;
    for (let m = 0; m < model_input.length; m++) {
      index = model_input[m].indexOf(" ");
      columns.push(model_input[m].substring(0, index));
    }

    // check if created at or updated at fields exist
    created_at_flag = false;
    updated_at_flag = false;
    pg_columns = [];
    pg_columns.push(columns[0]);
    if (columns.includes("createdAt")) {
      pg_columns.push("createdAt");
      created_at_flag = true;
    }
    if (columns.includes("updatedAt")) {
      pg_columns.push("updatedAt");
      updated_at_flag = true;
    }

    latest_update_date = "";
    latest_create_date = "";

    columns_string = columns.join(", ");
    pg_columns_String = pg_columns.join(", ");
    pgUpdateStatement = "";
    pgInsertStatement = "";
    no_data_flag = "";

    cb();
  };

  function json_key(object, key, k) {
    let out_array = [];

    if (typeof object === "undefined" || typeof object === null) {
      return null;
    }
    let json = JSON.parse(JSON.stringify(object));

    if (object === null) {
      return null;
    } else {
      if (
        JSON.stringify(json[key]) === "null" ||
        typeof json[key] === "undefined"
      ) {
        return null;
      } else if (
        JSON.stringify(json[key]).replace(/(\r\n|\n|\r)/gm, "").length > 0
      ) {
        // logic for different data types, i.e. date - moment, integer - convert number if needed
        if (model_data_type[k].indexOf("JSONB") !== -1) {
          return JSON.stringify(json[key]);
        } else if (model_data_type[k].indexOf("_") !== -1) {
          for (j in json[key]) {
            out_array.push(json[key][j]);
          }
          return out_array;
        } else if (model_data_type[k].indexOf("TIMESTAMP") !== -1) {
          if (json[key] === null) {
            print("this time is null");
          }
          return moment(
            JSON.stringify(json[key])
              .replace(/(\r\n|\n|\r)/gm, "")
              .replace(/"/gm, "")
          ).format();
        } else if (
          model_data_type[k].indexOf("INT") !== -1 ||
          model_data_type[k].indexOf("NUMERIC") !== -1
        ) {
          return Number(
            JSON.stringify(json[key])
              .replace(/(\r\n|\n|\r)/gm, "")
              .replace(/"/g, "")
          );
        } else {
          return JSON.stringify(json[key])
            .replace(/(\r\n|\n|\r)/gm, "")
            .replace(/"/gm, "");
        }
      } else {
        return key + " is an empty string";
      }
    }
  }

  function pgUpdateQuery(cols) {
    // Setup static beginning of query
    let query = ["UPDATE " + table_name];
    query.push("SET");

    // Create an array of the columns to update - skip first column which is for the WHERE
    let set = [];
    for (let col = 0; col < cols.length - 1; col++) {
      set.push(cols[col + 1] + " = ($" + (col + 2) + ")");
    }
    query.push(set.join(", "));

    // Add the WHERE statement to look up by id
    query.push("WHERE " + pg_columns[0] + " = $1");

    // Return a complete query string
    return query.join(" ");
  }

  let pgGenerate = async function (cb) {
    // create $1, $2, $3 for PG INSERT statement
    console.log("pgGenerate");
    let insert_values_array = Array.from({ length: columns.length }, (v, k) =>
      String("$" + (k + 1))
    );
    let insert_values_string = insert_values_array.join(", ");
    pgInsertStatement =
      "INSERT into " +
      table_name +
      " (" +
      columns_string +
      ") VALUES (" +
      insert_values_string +
      ")";
    // create $1, $2, $3 etc. for PG Update Statement
    pgUpdateStatement = pgUpdateQuery(columns);
    cb();
  };

  // Extraction of currently existing data in Postgres
  let pgExtract = async function (cb) {
    console.log("pgExtract");
    let pgUpdateText =
      "SELECT " +
      pg_columns_String +
      " from " +
      table_name +
      " WHERE updatedat IS NOT NULL ORDER BY updatedat DESC LIMIT 5";
    let pgCreateText =
      "SELECT " +
      pg_columns_String +
      " from " +
      table_name +
      " WHERE createdat IS NOT NULL ORDER BY createdat DESC LIMIT 5";
    let pgAllText =
      "SELECT " + pg_columns_String + " from " + table_name + " LIMIT 5";

    if (updated_at_flag) {
      pg_update_res = await PostgresConnection().query(pgUpdateText);
    }
    if (created_at_flag) {
      pg_create_res = await PostgresConnection().query(pgCreateText);
    }
    pg_all_res = await PostgresConnection().query(pgAllText);

    // console.log(pg_update_res);
    // console.log(pg_create_res);
    if (typeof pg_all_res.rows[0] === "undefined") {
      no_data_flag = "yes";
      console.log("no postgres data found");
    } else {
      if (updated_at_flag) {
        latest_update_date = moment(pg_update_res.rows[0].updatedat)
          .add(1, "seconds")
          .toISOString();
        console.log("latest updatedAt date: " + latest_update_date);
      }
      if (created_at_flag) {
        latest_create_date = moment(pg_create_res.rows[0].createdat)
          .add(1, "seconds")
          .toISOString();
        console.log("latest createdAt date: " + latest_create_date);
      }
    }
    cb();
  };

  async function mongoConnect(cb) {
    MongoConnection.connectToServer(function (err, client) {
      console.log("mongo connect");
      if (err) console.log(err);
      db = MongoConnection.getDb();
      cb();
    });
  }

  // -------------------------------------------------------
  async function startMongoExtract(queryType, cMessage, cb) {
    console.log(queryType);
    if (
      (no_data_flag == "yes" && queryType == "all_data") ||
      (no_data_flag != "yes" &&
        ((queryType == "existing_data" && updated_at_flag) ||
          (queryType == "new_data" && created_at_flag)))
    ) {
      let count = null;
      let found = null;
      let limit = 250;
      let max = 500000;

      while ((found === null || found == limit) && count < max) {
        console.log(found, limit);
        console.log(table_name);
        console.log(queryType);

        mongo_data = await new Promise((resolve, reject) => {
          if (queryType == "new_data" && created_at_flag) {
            if (table_name == "units") {
              db.collection("units")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "parent",
                      foreignField: "_id",
                      as: "parent1",
                    },
                  },
                  { $unwind: "$parent1" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "parent1.parent",
                      foreignField: "_id",
                      as: "parent2",
                    },
                  },
                  { $unwind: "$parent2" },

                  {
                    $match: {
                      createdAt: { $gte: new Date(latest_create_date) },
                    },
                  },

                  { $sort: { createdAt: 1 } },

                  { $skip: count === null ? 0 : count },

                  { $limit: limit },
                  {
                    $set: {
                      subcounty: {
                        $cond: [
                          {
                            $eq: ["$parent1.type", "Subcounty"],
                          },
                          "$parent1.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$name",
                              null,
                            ],
                          },
                        ],
                      },
                      county: {
                        $cond: [
                          { $eq: ["$parent2.type", "County"] },

                          "$parent2.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$parent1.name",
                              {
                                $cond: [
                                  {   
                                    $eq: ["$type", "County"],
                                  },
                                  "$name",
                                  null
                                ]
                              }
                            ],
                          },
                        ],
                      },
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            } else if (table_name == "tasks") {
              db.collection("tasks")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "unit",
                      foreignField: "_id",
                      as: "unit",
                    },
                  },
                  { $unwind: "$unit" },
                  {
                    $set: {
                      name: "$unit.name",
                      type: "$unit.type",
                    },
                  },
                  {
                    $match: {
                      createdAt: { $gte: new Date(latest_create_date) },
                    },
                  },

                  { $sort: { createdAt: 1 } },

                  { $skip: count === null ? 0 : count },

                  { $limit: limit },

                  {
                    $lookup: {
                      from: "units",
                      localField: "unit.parent",
                      foreignField: "_id",
                      as: "subcounty",
                    },
                  },

                  { $unwind: "$subcounty" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "subcounty.parent",
                      foreignField: "_id",
                      as: "county",
                    },
                  },
                  { $unwind: "$county" },
                  {
                    $set: {
                      subcounty: "$subcounty.name",
                      county: "$county.name",
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            }  else {
              db.collection(table_name)
                .find({ createdAt: { $gte: new Date(latest_create_date) } })
                .sort({ createdAt: 1 })
                .skip(count === null ? 0 : count)
                .limit(limit)
                .toArray((err, items) => {
                  resolve(items);
                });
            }
          } else if (queryType == "existing_data" && updated_at_flag) {
            if (table_name == "units") {
              db.collection("units")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "parent",
                      foreignField: "_id",
                      as: "parent1",
                    },
                  },
                  { $unwind: "$parent1" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "parent1.parent",
                      foreignField: "_id",
                      as: "parent2",
                    },
                  },
                  { $unwind: "$parent2" },

                  {
                    $match: {
                      updatedAt: { $gte: new Date(latest_update_date) },
                    },
                  },

                  { $sort: { updatedAt: 1 } },

                  { $skip: count === null ? 0 : count },

                  { $limit: limit },
                  {
                    $set: {
                      subcounty: {
                        $cond: [
                          {
                            $eq: ["$parent1.type", "Subcounty"],
                          },
                          "$parent1.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$name",
                              null,
                            ],
                          },
                        ],
                      },
                      county: {
                        $cond: [
                          { $eq: ["$parent2.type", "County"] },

                          "$parent2.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$parent1.name",
                              {
                                $cond: [
                                  {   
                                    $eq: ["$type", "County"],
                                  },
                                  "$name",
                                  null
                                ]
                              }
                            ],
                          },
                        ],
                      },
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            } else if (table_name == "tasks") {
              db.collection("tasks")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "unit",
                      foreignField: "_id",
                      as: "unit",
                    },
                  },
                  { $unwind: "$unit" },
                  {
                    $set: {
                      name: "$unit.name",
                      type: "$unit.type",
                    },
                  },
                  {
                    $match: {
                      updatedAt: { $gte: new Date(latest_update_date) },
                    },
                  },

                  { $sort: { updatedAt: 1 } },

                  { $skip: count === null ? 0 : count },

                  { $limit: limit },

                  {
                    $lookup: {
                      from: "units",
                      localField: "unit.parent",
                      foreignField: "_id",
                      as: "subcounty",
                    },
                  },
                  { $unwind: "$subcounty" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "subcounty.parent",
                      foreignField: "_id",
                      as: "county",
                    },
                  },
                  { $unwind: "$county" },
                  {
                    $set: {
                      subcounty: "$subcounty.name",
                      county: "$county.name",
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            }  else {
              db.collection(table_name)
                .find({ updatedAt: { $gte: new Date(latest_update_date) } })
                .sort({ updatedAt: 1 })
                .skip(count === null ? 0 : count)
                .limit(limit)
                .toArray((err, items) => {
                  resolve(items);
                });
            }
          } else if (queryType == "all_data") {
            if (table_name == "units") {
              db.collection("units")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "parent",
                      foreignField: "_id",
                      as: "parent1",
                    },
                  },
                  { $unwind: "$parent1" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "parent1.parent",
                      foreignField: "_id",
                      as: "parent2",
                    },
                  },
                  { $unwind: "$parent2" },

                  { $skip: count === null ? 0 : count },

                  { $limit: limit },

                  {
                    $set: {
                      subcounty: {
                        $cond: [
                          {
                            $eq: ["$parent1.type", "Subcounty"],
                          },
                          "$parent1.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$name",
                              null,
                            ],
                          },
                        ],
                      },
                      county: {
                        $cond: [
                          { $eq: ["$parent2.type", "County"] },

                          "$parent2.name",
                          {
                            $cond: [
                              {
                                $eq: ["$type", "Subcounty"],
                              },
                              "$parent1.name",
                              {
                                $cond: [
                                  {   
                                    $eq: ["$type", "County"],
                                  },
                                  "$name",
                                  null
                                ]
                              }
                            ],
                          },
                        ],
                      },
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            } else if (table_name == "tasks") {
              db.collection("tasks")
                .aggregate([
                  {
                    $lookup: {
                      from: "units",
                      localField: "unit",
                      foreignField: "_id",
                      as: "unit",
                    },
                  },
                  { $unwind: "$unit" },
                  {
                    $set: {
                      name: "$unit.name",
                      type: "$unit.type",
                    },
                  },
                  { $skip: count === null ? 0 : count },

                  { $limit: limit },

                  {
                    $lookup: {
                      from: "units",
                      localField: "unit.parent",
                      foreignField: "_id",
                      as: "subcounty",
                    },
                  },
                  { $unwind: "$subcounty" },

                  {
                    $lookup: {
                      from: "units",
                      localField: "subcounty.parent",
                      foreignField: "_id",
                      as: "county",
                    },
                  },
                  { $unwind: "$county" },
                  {
                    $set: {
                      subcounty: "$subcounty.name",
                      county: "$county.name",
                    },
                  },
                ])
                .toArray((err, items) => {
                  resolve(items);
                });
            }  else {
              db.collection(table_name)
                .find({})
                // .sort({ createdAt: 1 })
                .skip(count === null ? 0 : count)
                .limit(limit)
                .toArray((err, items) => {
                  resolve(items);
                });
            }
          } else {
            reject("error");
          }
        });

        let rows = [];

        for (let md in mongo_data) {
          try {
            let data_row = mongo_data[md];
            console.log(md);

            var insert_row = [];
            for (let j = 0; j < columns.length; j++) {
              var pmebsReportForm =
                data_row.pmebs == null ? null : data_row.pmebs.reportForm;
              var pmebsRequestForm =
                data_row.pmebs == null ? null : data_row.pmebs.requestForm;

              var cebsVerificationForm =
                data_row.cebs == null ? null : data_row.cebs.verificationForm;
              var cebsInvestigationForm =
                data_row.cebs == null ? null : data_row.cebs.investigationForm;
              var cebsResponseForm =
                data_row.cebs == null ? null : data_row.cebs.responseForm;
              var cebsEscalationForm =
                data_row.cebs == null ? null : data_row.cebs.escalationForm;

              var vebsVerificationForm =
                data_row.vebs == null ? null : data_row.vebs.verificationForm;
              var vebsInvestigationForm =
                data_row.vebs == null ? null : data_row.vebs.investigationForm;
              var vebsResponseForm =
                data_row.vebs == null ? null : data_row.vebs.responseForm;
              var vebsEscalationForm =
                data_row.vebs == null ? null : data_row.vebs.escalationForm;

              var hebsVerificationForm =
                data_row.hebs == null ? null : data_row.hebs.verificationForm;
              var hebsInvestigationForm =
                data_row.hebs == null ? null : data_row.hebs.investigationForm;
              var hebsResponseForm =
                data_row.hebs == null ? null : data_row.hebs.responseForm;
              var hebsEscalationForm =
                data_row.hebs == null ? null : data_row.hebs.escalationForm;

              var lebsVerificationForm =
                data_row.lebs == null ? null : data_row.lebs.verificationForm;
              var lebsInvestigationForm =
                data_row.lebs == null ? null : data_row.lebs.investigationForm;
              var lebsResponseForm =
                data_row.lebs == null ? null : data_row.lebs.responseForm;
              var lebsEscalationForm =
                data_row.lebs == null ? null : data_row.lebs.escalationForm;

              var unit = data_row.unit == null ? null : data_row.unit;

              // custom rules applied in this switch statement if needed, otherwise default will be used
              // -------------------------------------------------------
              switch (columns[j]) {
                // custom rule for extracting value from child level i.e. 'common' that is stored in the 'name' object
                case "common_name":
                  insert_row.push(json_key(data_row.name, "common", j));
                  break;
                case "userId":
                  insert_row.push(json_key(data_row, "user", j));
                  break;
                case "unitId":
                  insert_row.push(json_key(unit, "_id", j));
                  break;
                case "_to":
                  insert_row.push(json_key(data_row, "to", j));
                  break;
                case "_from":
                  insert_row.push(json_key(data_row, "from", j));
                  break;

                case "pmebs_reportForm_id":
                  insert_row.push(json_key(pmebsReportForm, "_id", j));
                  break;
                case "pmebs_reportForm_user":
                  insert_row.push(json_key(pmebsReportForm, "user", j));
                  break;
                case "pmebs_reportForm_dateDetected":
                  insert_row.push(json_key(pmebsReportForm, "dateDetected", j));
                  break;
                case "pmebs_reportForm_dateReported":
                  insert_row.push(json_key(pmebsReportForm, "dateReported", j));
                  break;
                case "pmebs_reportForm_description":
                  insert_row.push(json_key(pmebsReportForm, "description", j));
                  break;
                case "pmebs_reportForm_source":
                  insert_row.push(json_key(pmebsReportForm, "source", j));
                  break;
                case "pmebs_reportForm_unit":
                  insert_row.push(json_key(pmebsReportForm, "unit", j));
                  break;
                case "pmebs_reportForm_locality":
                  insert_row.push(json_key(pmebsReportForm, "locality", j));
                  break;
                case "pmebs_reportForm_createdAt":
                  insert_row.push(json_key(pmebsReportForm, "createdAt", j));
                  break;
                case "pmebs_reportForm_updatedAt":
                  insert_row.push(json_key(pmebsReportForm, "updatedAt", j));
                  break;

                //PMEBS Request Form

                case "pmebs_requestForm_id":
                  insert_row.push(json_key(pmebsRequestForm, "_id", j));
                  break;
                case "pmebs_requestForm_user":
                  insert_row.push(json_key(pmebsRequestForm, "user", j));
                  break;
                case "pmebs_requestForm_dateDetected":
                  insert_row.push(
                    json_key(pmebsRequestForm, "dateDetected", j)
                  );
                  break;
                case "pmebs_requestForm_dateReported":
                  insert_row.push(
                    json_key(pmebsRequestForm, "dateReported", j)
                  );
                  break;
                case "pmebs_requestForm_description":
                  insert_row.push(json_key(pmebsRequestForm, "description", j));
                  break;

                case "pmebs_requestForm_unit":
                  insert_row.push(json_key(pmebsRequestForm, "unit", j));
                  break;
                case "pmebs_requestForm_locality":
                  insert_row.push(json_key(pmebsRequestForm, "locality", j));
                  break;
                case "pmebs_requestForm_createdAt":
                  insert_row.push(json_key(pmebsRequestForm, "createdAt", j));
                  break;
                case "pmebs_requestForm_updatedAt":
                  insert_row.push(json_key(pmebsRequestForm, "updatedAt", j));
                  break;
                case "pmebs_id":
                  insert_row.push(json_key(data_row.pmebs, "_id", j));
                  break;
                case "pmebs_createdAt":
                  insert_row.push(json_key(data_row.pmebs, "createdAt", j));
                  break;
                case "pmebs_updatedAt":
                  insert_row.push(json_key(data_row.pmebs, "updatedAt", j));
                  break;

                //VEBS Forms
                case "vebs_createdAt":
                  insert_row.push(json_key(data_row.vebs, "createdAt", j));
                  break;
                case "vebs_updatedAt":
                  insert_row.push(json_key(data_row.vebs, "updatedAt", j));
                  break;
                case "vebs_id":
                  insert_row.push(json_key(data_row.vebs, "_id", j));
                  break;

                //VEBS Verification Form
                case "vebs_verificationForm_id":
                  insert_row.push(json_key(vebsVerificationForm, "_id", j));
                  break;
                case "vebs_verificationForm_user":
                  insert_row.push(json_key(vebsVerificationForm, "user", j));
                  break;
                case "vebs_verificationForm_description":
                  insert_row.push(
                    json_key(vebsVerificationForm, "description", j)
                  );
                  break;
                case "vebs_verificationForm_source":
                  insert_row.push(json_key(vebsVerificationForm, "source", j));
                  break;
                case "vebs_verificationForm_isMatchingSignal":
                  insert_row.push(
                    json_key(vebsVerificationForm, "isMatchingSignal", j)
                  );
                  break;
                case "vebs_verificationForm_updatedSignal":
                  insert_row.push(
                    json_key(vebsVerificationForm, "updatedSignal", j)
                  );
                  break;
                case "vebs_verificationForm_isReportedBefore":
                  insert_row.push(
                    json_key(vebsVerificationForm, "isReportedBefore", j)
                  );
                  break;
                case "vebs_verificationForm_dateHealthThreatStarted":
                  insert_row.push(
                    json_key(vebsVerificationForm, "dateHealthThreatStarted", j)
                  );
                  break;
                case "vebs_verificationForm_dateVerified":
                  insert_row.push(
                    json_key(vebsVerificationForm, "dateVerified", j)
                  );
                  break;
                case "vebs_verificationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(vebsVerificationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "vebs_verificationForm_informant":
                  insert_row.push(
                    json_key(vebsVerificationForm, "informant", j)
                  );
                  break;
                case "vebs_verificationForm_otherInformant":
                  insert_row.push(
                    json_key(vebsVerificationForm, "otherInformant", j)
                  );
                  break;
                case "vebs_verificationForm_additionalInformation":
                  insert_row.push(
                    json_key(vebsVerificationForm, "additionalInformation", j)
                  );
                  break;
                case "vebs_verificationForm_isThreatStillExisting":
                  insert_row.push(
                    json_key(vebsVerificationForm, "isThreatStillExisting", j)
                  );
                  break;
                case "vebs_verificationForm_threatTo":
                  insert_row.push(
                    json_key(vebsVerificationForm, "threatTo", j)
                  );
                  break;
                case "vebs_verificationForm_via":
                  insert_row.push(json_key(vebsVerificationForm, "via", j));
                  break;
                case "vebs_verificationForm_createdAt":
                  insert_row.push(
                    json_key(vebsVerificationForm, "createdAt", j)
                  );
                  break;
                case "vebs_verificationForm_updatedAt":
                  insert_row.push(
                    json_key(vebsVerificationForm, "updatedAt", j)
                  );
                  break;

                //VEBS Investigation Form

                case "vebs_investigationForm_id":
                  insert_row.push(json_key(vebsInvestigationForm, "_id", j));
                  break;
                case "vebs_investigationForm_symptoms":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "symptoms", j)
                  );
                  break;
                case "vebs_investigationForm_user":
                  insert_row.push(json_key(vebsInvestigationForm, "user", j));
                  break;
                case "vebs_investigationForm_humansCases":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "humansCases", j)
                  );
                  break;
                case "vebs_investigationForm_humansCasesHospitalized":
                  insert_row.push(
                    json_key(
                      vebsInvestigationForm,
                      "humansCasesHospitalized",
                      j
                    )
                  );
                  break;
                case "vebs_investigationForm_humansDead":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "humansDead", j)
                  );
                  break;
                case "vebs_investigationForm_animalsCases":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "animalsCases", j)
                  );
                  break;
                case "vebs_investigationForm_animalsDead":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "animalsDead", j)
                  );
                  break;
                case "vebs_investigationForm_isCauseKnown":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "isCauseKnown", j)
                  );
                  break;
                case "vebs_investigationForm_cause":
                  insert_row.push(json_key(vebsInvestigationForm, "cause", j));
                  break;
                case "vebs_investigationForm_isLabSamplesCollected":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "isLabSamplesCollected", j)
                  );
                  break;
                case "vebs_investigationForm_labResults":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "labResults", j)
                  );
                  break;
                case "vebs_investigationForm_dateEventStarted":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "dateEventStarted", j)
                  );
                  break;
                case "vebs_investigationForm_dateInvestigationStarted":
                  insert_row.push(
                    json_key(
                      vebsInvestigationForm,
                      "dateInvestigationStarted",
                      j
                    )
                  );
                  break;
                case "vebs_investigationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "vebs_investigationForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "vebs_investigationForm_dateLabResultsReceived":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "dateLabResultsReceived", j)
                  );
                  break;
                case "vebs_investigationForm_dateSampleCollected":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "dateSampleCollected", j)
                  );
                  break;
                case "vebs_investigationForm_isNewCasedReportedFromInitialArea":
                  insert_row.push(
                    json_key(
                      vebsInvestigationForm,
                      "isNewCasedReportedFromInitialArea",
                      j
                    )
                  );
                  break;
                case "vebs_investigationForm_isNewCasedReportedFromNewAreas":
                  insert_row.push(
                    json_key(
                      vebsInvestigationForm,
                      "isNewCasedReportedFromNewAreas",
                      j
                    )
                  );
                  break;
                case "vebs_investigationForm_isEventSettingPromotingSpread":
                  insert_row.push(
                    json_key(
                      vebsInvestigationForm,
                      "isEventSettingPromotingSpread",
                      j
                    )
                  );
                  break;
                case "vebs_investigationForm_additionalInformation":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "additionalInformation", j)
                  );
                  break;
                case "vebs_investigationForm_riskClassification":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "riskClassification", j)
                  );
                  break;
                case "vebs_investigationForm_via":
                  insert_row.push(json_key(vebsInvestigationForm, "via", j));
                  break;
                case "vebs_investigationForm_responseActivities":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "responseActivities", j)
                  );
                  break;
                case "vebs_investigationForm_createdAt":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "createdAt", j)
                  );
                  break;
                case "vebs_investigationForm_updatedAt":
                  insert_row.push(
                    json_key(vebsInvestigationForm, "updatedAt", j)
                  );
                  break;

                //VEBS Response Form
                case "vebs_responseForm_id":
                  insert_row.push(json_key(vebsResponseForm, "_id", j));
                  break;
                case "vebs_responseForm_user":
                  insert_row.push(json_key(vebsResponseForm, "user", j));
                  break;
                case "vebs_responseForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(vebsResponseForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "vebs_responseForm_dateResponseStarted":
                  insert_row.push(
                    json_key(vebsResponseForm, "dateResponseStarted", j)
                  );
                  break;
                case "vebs_responseForm_dateEscalated":
                  insert_row.push(
                    json_key(vebsResponseForm, "dateEscalated", j)
                  );
                  break;
                case "vebs_responseForm_dateOfReport":
                  insert_row.push(
                    json_key(vebsResponseForm, "dateOfReport", j)
                  );
                  break;
                case "vebs_responseForm_responseActivities":
                  insert_row.push(
                    json_key(vebsResponseForm, "responseActivities", j)
                  );
                  break;
                case "vebs_responseForm_otherResponseActivity":
                  insert_row.push(
                    json_key(vebsResponseForm, "otherResponseActivity", j)
                  );
                  break;
                case "vebs_responseForm_outcomeOfResponse":
                  insert_row.push(
                    json_key(vebsResponseForm, "outcomeOfResponse", j)
                  );
                  break;
                case "vebs_responseForm_recommendations":
                  insert_row.push(
                    json_key(vebsResponseForm, "recommendations", j)
                  );
                  break;
                case "vebs_responseForm_additionalInformation":
                  insert_row.push(
                    json_key(vebsResponseForm, "additionalInformation", j)
                  );
                  break;
                case "vebs_responseForm_via":
                  insert_row.push(json_key(vebsResponseForm, "via", j));
                  break;
                case "vebs_responseForm_createdAt":
                  insert_row.push(json_key(vebsResponseForm, "createdAt", j));
                  break;
                case "vebs_responseForm_updatedAt":
                  insert_row.push(json_key(vebsResponseForm, "updatedAt", j));
                  break;

                //VEBS Escalation Form
                case "vebs_escalationForm_id":
                  insert_row.push(json_key(vebsEscalationForm, "_id", j));
                  break;
                case "vebs_escalationForm_user":
                  insert_row.push(json_key(vebsEscalationForm, "user", j));
                  break;
                case "vebs_escalationForm_dateResponseStarted":
                  insert_row.push(
                    json_key(vebsEscalationForm, "dateResponseStarted", j)
                  );
                  break;
                case "vebs_escalationForm_dateEscalated":
                  insert_row.push(
                    json_key(vebsEscalationForm, "dateEscalated", j)
                  );
                  break;
                case "vebs_escalationForm_reason":
                  insert_row.push(json_key(vebsEscalationForm, "reason", j));
                  break;
                case "vebs_escalationForm_reasonOther":
                  insert_row.push(
                    json_key(vebsEscalationForm, "reasonOther", j)
                  );
                  break;
                case "vebs_escalationForm_via":
                  insert_row.push(json_key(vebsEscalationForm, "via", j));
                  break;

                case "vebs_escalationForm_createdAt":
                  insert_row.push(json_key(vebsEscalationForm, "createdAt", j));
                  break;
                case "vebs_escalationForm_updatedAt":
                  insert_row.push(json_key(vebsEscalationForm, "updatedAt", j));
                  break;

                //CEBS Forms
                case "cebs_createdAt":
                  insert_row.push(json_key(data_row.cebs, "createdAt", j));
                  break;
                case "cebs_updatedAt":
                  insert_row.push(json_key(data_row.cebs, "updatedAt", j));
                  break;
                case "cebs_id":
                  insert_row.push(json_key(data_row.cebs, "_id", j));
                  break;

                //CEBS Verification Form
                case "cebs_verificationForm_id":
                  insert_row.push(json_key(cebsVerificationForm, "_id", j));
                  break;
                case "cebs_verificationForm_user":
                  insert_row.push(json_key(cebsVerificationForm, "user", j));
                  break;
                case "cebs_verificationForm_description":
                  insert_row.push(
                    json_key(cebsVerificationForm, "description", j)
                  );
                  break;
                case "cebs_verificationForm_source":
                  insert_row.push(json_key(cebsVerificationForm, "source", j));
                  break;
                case "cebs_verificationForm_isMatchingSignal":
                  insert_row.push(
                    json_key(cebsVerificationForm, "isMatchingSignal", j)
                  );
                  break;
                case "cebs_verificationForm_updatedSignal":
                  insert_row.push(
                    json_key(cebsVerificationForm, "updatedSignal", j)
                  );
                  break;
                case "cebs_verificationForm_isReportedBefore":
                  insert_row.push(
                    json_key(cebsVerificationForm, "isReportedBefore", j)
                  );
                  break;
                case "cebs_verificationForm_dateHealthThreatStarted":
                  insert_row.push(
                    json_key(cebsVerificationForm, "dateHealthThreatStarted", j)
                  );
                  break;
                case "cebs_verificationForm_dateVerified":
                  insert_row.push(
                    json_key(cebsVerificationForm, "dateVerified", j)
                  );
                  break;
                case "cebs_verificationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(cebsVerificationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "cebs_verificationForm_informant":
                  insert_row.push(
                    json_key(cebsVerificationForm, "informant", j)
                  );
                  break;
                case "cebs_verificationForm_otherInformant":
                  insert_row.push(
                    json_key(cebsVerificationForm, "otherInformant", j)
                  );
                  break;
                case "cebs_verificationForm_additionalInformation":
                  insert_row.push(
                    json_key(cebsVerificationForm, "additionalInformation", j)
                  );
                  break;
                case "cebs_verificationForm_isThreatStillExisting":
                  insert_row.push(
                    json_key(cebsVerificationForm, "isThreatStillExisting", j)
                  );
                  break;
                case "cebs_verificationForm_threatTo":
                  insert_row.push(
                    json_key(cebsVerificationForm, "threatTo", j)
                  );
                  break;
                case "cebs_verificationForm_via":
                  insert_row.push(json_key(cebsVerificationForm, "via", j));
                  break;
                case "cebs_verificationForm_createdAt":
                  insert_row.push(
                    json_key(cebsVerificationForm, "createdAt", j)
                  );
                  break;
                case "cebs_verificationForm_updatedAt":
                  insert_row.push(
                    json_key(cebsVerificationForm, "updatedAt", j)
                  );
                  break;

                //CEBS Investigation Form

                case "cebs_investigationForm_id":
                  insert_row.push(json_key(cebsInvestigationForm, "_id", j));
                  break;
                case "cebs_investigationForm_symptoms":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "symptoms", j)
                  );
                  break;
                case "cebs_investigationForm_user":
                  insert_row.push(json_key(cebsInvestigationForm, "user", j));
                  break;
                case "cebs_investigationForm_humansCases":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "humansCases", j)
                  );
                  break;
                case "cebs_investigationForm_humansCasesHospitalized":
                  insert_row.push(
                    json_key(
                      cebsInvestigationForm,
                      "humansCasesHospitalized",
                      j
                    )
                  );
                  break;
                case "cebs_investigationForm_humansDead":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "humansDead", j)
                  );
                  break;
                case "cebs_investigationForm_animalsCases":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "animalsCases", j)
                  );
                  break;
                case "cebs_investigationForm_animalsDead":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "animalsDead", j)
                  );
                  break;
                case "cebs_investigationForm_isCauseKnown":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "isCauseKnown", j)
                  );
                  break;
                case "cebs_investigationForm_cause":
                  insert_row.push(json_key(cebsInvestigationForm, "cause", j));
                  break;
                case "cebs_investigationForm_isLabSamplesCollected":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "isLabSamplesCollected", j)
                  );
                  break;
                case "cebs_investigationForm_labResults":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "labResults", j)
                  );
                  break;
                case "cebs_investigationForm_dateEventStarted":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "dateEventStarted", j)
                  );
                  break;
                case "cebs_investigationForm_dateInvestigationStarted":
                  insert_row.push(
                    json_key(
                      cebsInvestigationForm,
                      "dateInvestigationStarted",
                      j
                    )
                  );
                  break;
                case "cebs_investigationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "cebs_investigationForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "cebs_investigationForm_dateLabResultsReceived":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "dateLabResultsReceived", j)
                  );
                  break;
                case "cebs_investigationForm_dateSampleCollected":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "dateSampleCollected", j)
                  );
                  break;
                case "cebs_investigationForm_isNewCasedReportedFromInitialArea":
                  insert_row.push(
                    json_key(
                      cebsInvestigationForm,
                      "isNewCasedReportedFromInitialArea",
                      j
                    )
                  );
                  break;
                case "cebs_investigationForm_isNewCasedReportedFromNewAreas":
                  insert_row.push(
                    json_key(
                      cebsInvestigationForm,
                      "isNewCasedReportedFromNewAreas",
                      j
                    )
                  );
                  break;
                case "cebs_investigationForm_isEventSettingPromotingSpread":
                  insert_row.push(
                    json_key(
                      cebsInvestigationForm,
                      "isEventSettingPromotingSpread",
                      j
                    )
                  );
                  break;
                case "cebs_investigationForm_additionalInformation":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "additionalInformation", j)
                  );
                  break;
                case "cebs_investigationForm_riskClassification":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "riskClassification", j)
                  );
                  break;
                case "cebs_investigationForm_via":
                  insert_row.push(json_key(cebsInvestigationForm, "via", j));
                  break;
                case "cebs_investigationForm_responseActivities":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "responseActivities", j)
                  );
                  break;
                case "cebs_investigationForm_createdAt":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "createdAt", j)
                  );
                  break;
                case "cebs_investigationForm_updatedAt":
                  insert_row.push(
                    json_key(cebsInvestigationForm, "updatedAt", j)
                  );
                  break;

                //CEBS Response Form
                case "cebs_responseForm_id":
                  insert_row.push(json_key(cebsResponseForm, "_id", j));
                  break;
                case "cebs_responseForm_user":
                  insert_row.push(json_key(cebsResponseForm, "user", j));
                  break;
                case "cebs_responseForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(cebsResponseForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "cebs_responseForm_dateResponseStarted":
                  insert_row.push(
                    json_key(cebsResponseForm, "dateResponseStarted", j)
                  );
                  break;
                case "cebs_responseForm_dateEscalated":
                  insert_row.push(
                    json_key(cebsResponseForm, "dateEscalated", j)
                  );
                  break;
                case "cebs_responseForm_dateOfReport":
                  insert_row.push(
                    json_key(cebsResponseForm, "dateOfReport", j)
                  );
                  break;
                case "cebs_responseForm_responseActivities":
                  insert_row.push(
                    json_key(cebsResponseForm, "responseActivities", j)
                  );
                  break;
                case "cebs_responseForm_otherResponseActivity":
                  insert_row.push(
                    json_key(cebsResponseForm, "otherResponseActivity", j)
                  );
                  break;
                case "cebs_responseForm_outcomeOfResponse":
                  insert_row.push(
                    json_key(cebsResponseForm, "outcomeOfResponse", j)
                  );
                  break;
                case "cebs_responseForm_recommendations":
                  insert_row.push(
                    json_key(cebsResponseForm, "recommendations", j)
                  );
                  break;
                case "cebs_responseForm_additionalInformation":
                  insert_row.push(
                    json_key(cebsResponseForm, "additionalInformation", j)
                  );
                  break;
                case "cebs_responseForm_via":
                  insert_row.push(json_key(cebsResponseForm, "via", j));
                  break;
                case "cebs_responseForm_createdAt":
                  insert_row.push(json_key(cebsResponseForm, "createdAt", j));
                  break;
                case "cebs_responseForm_updatedAt":
                  insert_row.push(json_key(cebsResponseForm, "updatedAt", j));
                  break;

                //CEBS Escalation Form
                case "cebs_escalationForm_id":
                  insert_row.push(json_key(cebsEscalationForm, "_id", j));
                  break;
                case "cebs_escalationForm_user":
                  insert_row.push(json_key(cebsEscalationForm, "user", j));
                  break;
                case "cebs_escalationForm_dateResponseStarted":
                  insert_row.push(
                    json_key(cebsEscalationForm, "dateResponseStarted", j)
                  );
                  break;
                case "cebs_escalationForm_dateEscalated":
                  insert_row.push(
                    json_key(cebsEscalationForm, "dateEscalated", j)
                  );
                  break;
                case "cebs_escalationForm_reason":
                  insert_row.push(json_key(cebsEscalationForm, "reason", j));
                  break;
                case "cebs_escalationForm_reasonOther":
                  insert_row.push(
                    json_key(cebsEscalationForm, "reasonOther", j)
                  );
                  break;
                case "cebs_escalationForm_via":
                  insert_row.push(json_key(cebsEscalationForm, "via", j));
                  break;

                case "cebs_escalationForm_createdAt":
                  insert_row.push(json_key(cebsEscalationForm, "createdAt", j));
                  break;
                case "cebs_escalationForm_updatedAt":
                  insert_row.push(json_key(cebsEscalationForm, "updatedAt", j));
                  break;

                //HEBS Forms
                case "hebs_createdAt":
                  insert_row.push(json_key(data_row.hebs, "createdAt", j));
                  break;
                case "hebs_updatedAt":
                  insert_row.push(json_key(data_row.hebs, "updatedAt", j));
                  break;
                case "hebs_id":
                  insert_row.push(json_key(data_row.hebs, "_id", j));
                  break;

                //HEBS Verification Form
                case "hebs_verificationForm_id":
                  insert_row.push(json_key(hebsVerificationForm, "_id", j));
                  break;
                case "hebs_verificationForm_user":
                  insert_row.push(json_key(hebsVerificationForm, "user", j));
                  break;
                case "hebs_verificationForm_description":
                  insert_row.push(
                    json_key(hebsVerificationForm, "description", j)
                  );
                  break;
                case "hebs_verificationForm_source":
                  insert_row.push(json_key(hebsVerificationForm, "source", j));
                  break;
                case "hebs_verificationForm_isMatchingSignal":
                  insert_row.push(
                    json_key(hebsVerificationForm, "isMatchingSignal", j)
                  );
                  break;
                case "hebs_verificationForm_updatedSignal":
                  insert_row.push(
                    json_key(hebsVerificationForm, "updatedSignal", j)
                  );
                  break;
                case "hebs_verificationForm_isReportedBefore":
                  insert_row.push(
                    json_key(hebsVerificationForm, "isReportedBefore", j)
                  );
                  break;
                case "hebs_verificationForm_dateHealthThreatStarted":
                  insert_row.push(
                    json_key(hebsVerificationForm, "dateHealthThreatStarted", j)
                  );
                  break;
                case "hebs_verificationForm_dateVerified":
                  insert_row.push(
                    json_key(hebsVerificationForm, "dateVerified", j)
                  );
                  break;
                case "hebs_verificationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(hebsVerificationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "hebs_verificationForm_informant":
                  insert_row.push(
                    json_key(hebsVerificationForm, "informant", j)
                  );
                  break;
                case "hebs_verificationForm_otherInformant":
                  insert_row.push(
                    json_key(hebsVerificationForm, "otherInformant", j)
                  );
                  break;
                case "hebs_verificationForm_additionalInformation":
                  insert_row.push(
                    json_key(hebsVerificationForm, "additionalInformation", j)
                  );
                  break;
                case "hebs_verificationForm_isThreatStillExisting":
                  insert_row.push(
                    json_key(hebsVerificationForm, "isThreatStillExisting", j)
                  );
                  break;
                case "hebs_verificationForm_threatTo":
                  insert_row.push(
                    json_key(hebsVerificationForm, "threatTo", j)
                  );
                  break;
                case "hebs_verificationForm_via":
                  insert_row.push(json_key(hebsVerificationForm, "via", j));
                  break;
                case "hebs_verificationForm_createdAt":
                  insert_row.push(
                    json_key(hebsVerificationForm, "createdAt", j)
                  );
                  break;
                case "hebs_verificationForm_updatedAt":
                  insert_row.push(
                    json_key(hebsVerificationForm, "updatedAt", j)
                  );
                  break;

                //HEBS Investigation Form

                case "hebs_investigationForm_id":
                  insert_row.push(json_key(hebsInvestigationForm, "_id", j));
                  break;
                case "hebs_investigationForm_symptoms":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "symptoms", j)
                  );
                  break;
                case "hebs_investigationForm_user":
                  insert_row.push(json_key(hebsInvestigationForm, "user", j));
                  break;
                case "hebs_investigationForm_humansCases":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "humansCases", j)
                  );
                  break;
                case "hebs_investigationForm_humansCasesHospitalized":
                  insert_row.push(
                    json_key(
                      hebsInvestigationForm,
                      "humansCasesHospitalized",
                      j
                    )
                  );
                  break;
                case "hebs_investigationForm_humansDead":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "humansDead", j)
                  );
                  break;
                case "hebs_investigationForm_animalsCases":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "animalsCases", j)
                  );
                  break;
                case "hebs_investigationForm_animalsDead":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "animalsDead", j)
                  );
                  break;
                case "hebs_investigationForm_isCauseKnown":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "isCauseKnown", j)
                  );
                  break;
                case "hebs_investigationForm_cause":
                  insert_row.push(json_key(hebsInvestigationForm, "cause", j));
                  break;
                case "hebs_investigationForm_isLabSamplesCollected":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "isLabSamplesCollected", j)
                  );
                  break;
                case "hebs_investigationForm_labResults":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "labResults", j)
                  );
                  break;
                case "hebs_investigationForm_dateEventStarted":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "dateEventStarted", j)
                  );
                  break;
                case "hebs_investigationForm_dateInvestigationStarted":
                  insert_row.push(
                    json_key(
                      hebsInvestigationForm,
                      "dateInvestigationStarted",
                      j
                    )
                  );
                  break;
                case "hebs_investigationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "hebs_investigationForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "hebs_investigationForm_dateLabResultsReceived":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "dateLabResultsReceived", j)
                  );
                  break;
                case "hebs_investigationForm_dateSampleCollected":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "dateSampleCollected", j)
                  );
                  break;
                case "hebs_investigationForm_isNewCasedReportedFromInitialArea":
                  insert_row.push(
                    json_key(
                      hebsInvestigationForm,
                      "isNewCasedReportedFromInitialArea",
                      j
                    )
                  );
                  break;
                case "hebs_investigationForm_isNewCasedReportedFromNewAreas":
                  insert_row.push(
                    json_key(
                      hebsInvestigationForm,
                      "isNewCasedReportedFromNewAreas",
                      j
                    )
                  );
                  break;
                case "hebs_investigationForm_isEventSettingPromotingSpread":
                  insert_row.push(
                    json_key(
                      hebsInvestigationForm,
                      "isEventSettingPromotingSpread",
                      j
                    )
                  );
                  break;
                case "hebs_investigationForm_additionalInformation":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "additionalInformation", j)
                  );
                  break;
                case "hebs_investigationForm_riskClassification":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "riskClassification", j)
                  );
                  break;
                case "hebs_investigationForm_via":
                  insert_row.push(json_key(hebsInvestigationForm, "via", j));
                  break;
                case "hebs_investigationForm_responseActivities":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "responseActivities", j)
                  );
                  break;
                case "hebs_investigationForm_createdAt":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "createdAt", j)
                  );
                  break;
                case "hebs_investigationForm_updatedAt":
                  insert_row.push(
                    json_key(hebsInvestigationForm, "updatedAt", j)
                  );
                  break;

                //HEBS Response Form
                case "hebs_responseForm_id":
                  insert_row.push(json_key(hebsResponseForm, "_id", j));
                  break;
                case "hebs_responseForm_user":
                  insert_row.push(json_key(hebsResponseForm, "user", j));
                  break;
                case "hebs_responseForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(hebsResponseForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "hebs_responseForm_dateResponseStarted":
                  insert_row.push(
                    json_key(hebsResponseForm, "dateResponseStarted", j)
                  );
                  break;
                case "hebs_responseForm_dateEscalated":
                  insert_row.push(
                    json_key(hebsResponseForm, "dateEscalated", j)
                  );
                  break;
                case "hebs_responseForm_dateOfReport":
                  insert_row.push(
                    json_key(hebsResponseForm, "dateOfReport", j)
                  );
                  break;
                case "hebs_responseForm_responseActivities":
                  insert_row.push(
                    json_key(hebsResponseForm, "responseActivities", j)
                  );
                  break;
                case "hebs_responseForm_otherResponseActivity":
                  insert_row.push(
                    json_key(hebsResponseForm, "otherResponseActivity", j)
                  );
                  break;
                case "hebs_responseForm_outcomeOfResponse":
                  insert_row.push(
                    json_key(hebsResponseForm, "outcomeOfResponse", j)
                  );
                  break;
                case "hebs_responseForm_recommendations":
                  insert_row.push(
                    json_key(hebsResponseForm, "recommendations", j)
                  );
                  break;
                case "hebs_responseForm_additionalInformation":
                  insert_row.push(
                    json_key(hebsResponseForm, "additionalInformation", j)
                  );
                  break;
                case "hebs_responseForm_via":
                  insert_row.push(json_key(hebsResponseForm, "via", j));
                  break;
                case "hebs_responseForm_createdAt":
                  insert_row.push(json_key(hebsResponseForm, "createdAt", j));
                  break;
                case "hebs_responseForm_updatedAt":
                  insert_row.push(json_key(hebsResponseForm, "updatedAt", j));
                  break;

                //HEBS Escalation Form
                case "hebs_escalationForm_id":
                  insert_row.push(json_key(hebsEscalationForm, "_id", j));
                  break;
                case "hebs_escalationForm_user":
                  insert_row.push(json_key(hebsEscalationForm, "user", j));
                  break;
                case "hebs_escalationForm_dateResponseStarted":
                  insert_row.push(
                    json_key(hebsEscalationForm, "dateResponseStarted", j)
                  );
                  break;
                case "hebs_escalationForm_dateEscalated":
                  insert_row.push(
                    json_key(hebsEscalationForm, "dateEscalated", j)
                  );
                  break;
                case "hebs_escalationForm_reason":
                  insert_row.push(json_key(hebsEscalationForm, "reason", j));
                  break;
                case "hebs_escalationForm_reasonOther":
                  insert_row.push(
                    json_key(hebsEscalationForm, "reasonOther", j)
                  );
                  break;
                case "hebs_escalationForm_via":
                  insert_row.push(json_key(hebsEscalationForm, "via", j));
                  break;

                case "hebs_escalationForm_createdAt":
                  insert_row.push(json_key(hebsEscalationForm, "createdAt", j));
                  break;
                case "hebs_escalationForm_updatedAt":
                  insert_row.push(json_key(hebsEscalationForm, "updatedAt", j));
                  break;

                //LEBS Forms
                case "lebs_createdAt":
                  insert_row.push(json_key(data_row.lebs, "createdAt", j));
                  break;
                case "lebs_updatedAt":
                  insert_row.push(json_key(data_row.lebs, "updatedAt", j));
                  break;
                case "lebs_id":
                  insert_row.push(json_key(data_row.lebs, "_id", j));
                  break;

                //LEBS Verification Form
                case "lebs_verificationForm_id":
                  insert_row.push(json_key(lebsVerificationForm, "_id", j));
                  break;
                case "lebs_verificationForm_user":
                  insert_row.push(json_key(lebsVerificationForm, "user", j));
                  break;
                case "lebs_verificationForm_description":
                  insert_row.push(
                    json_key(lebsVerificationForm, "description", j)
                  );
                  break;
                case "lebs_verificationForm_source":
                  insert_row.push(json_key(lebsVerificationForm, "source", j));
                  break;
                case "lebs_verificationForm_isMatchingSignal":
                  insert_row.push(
                    json_key(lebsVerificationForm, "isMatchingSignal", j)
                  );
                  break;
                case "lebs_verificationForm_updatedSignal":
                  insert_row.push(
                    json_key(lebsVerificationForm, "updatedSignal", j)
                  );
                  break;
                case "lebs_verificationForm_isReportedBefore":
                  insert_row.push(
                    json_key(lebsVerificationForm, "isReportedBefore", j)
                  );
                  break;
                case "lebs_verificationForm_dateHealthThreatStarted":
                  insert_row.push(
                    json_key(lebsVerificationForm, "dateHealthThreatStarted", j)
                  );
                  break;
                case "lebs_verificationForm_dateVerified":
                  insert_row.push(
                    json_key(lebsVerificationForm, "dateVerified", j)
                  );
                  break;
                case "lebs_verificationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(lebsVerificationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "lebs_verificationForm_informant":
                  insert_row.push(
                    json_key(lebsVerificationForm, "informant", j)
                  );
                  break;
                case "lebs_verificationForm_otherInformant":
                  insert_row.push(
                    json_key(lebsVerificationForm, "otherInformant", j)
                  );
                  break;
                case "lebs_verificationForm_additionalInformation":
                  insert_row.push(
                    json_key(lebsVerificationForm, "additionalInformation", j)
                  );
                  break;
                case "lebs_verificationForm_isStillHappening":
                  insert_row.push(
                    json_key(lebsVerificationForm, "isStillHappening", j)
                  );
                  break;
                case "lebs_verificationForm_via":
                  insert_row.push(json_key(lebsVerificationForm, "via", j));
                  break;
                case "lebs_verificationForm_createdAt":
                  insert_row.push(
                    json_key(lebsVerificationForm, "createdAt", j)
                  );
                  break;
                case "lebs_verificationForm_updatedAt":
                  insert_row.push(
                    json_key(lebsVerificationForm, "updatedAt", j)
                  );
                  break;

                //LEBS Investigation Form

                case "lebs_investigationForm_id":
                  insert_row.push(json_key(lebsInvestigationForm, "_id", j));
                  break;
                case "lebs_investigationForm_symptoms":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "symptoms", j)
                  );
                  break;
                case "lebs_investigationForm_user":
                  insert_row.push(json_key(lebsInvestigationForm, "user", j));
                  break;
                case "lebs_investigationForm_symptomsOther":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "symptomsOther", j)
                  );
                  break;
                case "lebs_investigationForm_isSamplesCollected":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "isSamplesCollected", j)
                  );
                  break;
                case "lebs_investigationForm_labResults":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "labResults", j)
                  );
                  break;

                case "lebs_investigationForm_dateEventStarted":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "dateEventStarted", j)
                  );
                  break;
                case "lebs_investigationForm_dateInvestigationStarted":
                  insert_row.push(
                    json_key(
                      lebsInvestigationForm,
                      "dateInvestigationStarted",
                      j
                    )
                  );
                  break;
                case "lebs_investigationForm_dateSCDSCInformed":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "dateSCDSCInformed", j)
                  );
                  break;
                case "lebs_investigationForm_dateRRTInformed":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "dateRRTInformed", j)
                  );
                  break;
                case "lebs_investigationForm_dateLabResultsReceived":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "dateLabResultsReceived", j)
                  );
                  break;
                case "lebs_investigationForm_dateSampleCollected":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "dateSampleCollected", j)
                  );
                  break;
                case "lebs_investigationForm_isCovid19WorkingCaseDefinitionMet":
                  insert_row.push(
                    json_key(
                      lebsInvestigationForm,
                      "isCovid19WorkingCaseDefinitionMet",
                      j
                    )
                  );
                  break;

                case "lebs_investigationForm_isEventSettingPromotingSpread":
                  insert_row.push(
                    json_key(
                      lebsInvestigationForm,
                      "isEventSettingPromotingSpread",
                      j
                    )
                  );
                  break;
                case "lebs_investigationForm_additionalInformation":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "additionalInformation", j)
                  );
                  break;
                case "lebs_investigationForm_measureHandHygiene":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "measureHandHygiene", j)
                  );
                  break;
                case "lebs_investigationForm_measureTempScreening":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "measureTempScreening", j)
                  );
                  break;
                case "lebs_investigationForm_measurePhysicalDistancing":
                  insert_row.push(
                    json_key(
                      lebsInvestigationForm,
                      "measurePhysicalDistancing",
                      j
                    )
                  );
                  break;
                case "lebs_investigationForm_measureUseOfMasks":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "measureUseOfMasks", j)
                  );
                  break;
                case "lebs_investigationForm_measureVentilation":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "measureVentilation", j)
                  );
                  break;
                case "lebs_investigationForm_measureSocialDistancing":
                  insert_row.push(
                    json_key(
                      lebsInvestigationForm,
                      "measureSocialDistancing",
                      j
                    )
                  );
                  break;
                case "lebs_investigationForm_additionalInformation":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "additionalInformation", j)
                  );
                  break;
                case "lebs_investigationForm_riskClassification":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "riskClassification", j)
                  );
                  break;
                case "lebs_investigationForm_via":
                  insert_row.push(json_key(lebsInvestigationForm, "via", j));
                  break;
                case "lebs_investigationForm_responseActivities":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "responseActivities", j)
                  );
                  break;
                case "lebs_investigationForm_createdAt":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "createdAt", j)
                  );
                  break;
                case "lebs_investigationForm_updatedAt":
                  insert_row.push(
                    json_key(lebsInvestigationForm, "updatedAt", j)
                  );
                  break;

                //LEBS Response Form
                case "lebs_responseForm_id":
                  insert_row.push(json_key(lebsResponseForm, "_id", j));
                  break;
                case "lebs_responseForm_user":
                  insert_row.push(json_key(lebsResponseForm, "user", j));
                  break;
                case "lebs_responseForm_dateSCMOHInformed":
                  insert_row.push(
                    json_key(lebsResponseForm, "dateSCMOHInformed", j)
                  );
                  break;
                case "lebs_responseForm_dateResponseStarted":
                  insert_row.push(
                    json_key(lebsResponseForm, "dateResponseStarted", j)
                  );
                  break;
                case "lebs_responseForm_dateSamplesCollected":
                  insert_row.push(
                    json_key(lebsResponseForm, "dateSamplesCollected", j)
                  );
                  break;
                case "lebs_responseForm_dateOfTestResults":
                  insert_row.push(
                    json_key(lebsResponseForm, "dateOfTestResults", j)
                  );
                  break;
                case "lebs_responseForm_isCovid19WorkingCaseDefinitionMet":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "isCovid19WorkingCaseDefinitionMet",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_isCIFFilledAndSamplesCollected":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "isCIFFilledAndSamplesCollected",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_reasonsNoSampleCollectedOther":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "reasonsNoSampleCollectedOther",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_responseActivitiesOther":
                  insert_row.push(
                    json_key(lebsResponseForm, "responseActivitiesOther", j)
                  );
                  break;
                case "lebs_responseForm_isHumansQuarantinedFollowedUp":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "isHumansQuarantinedFollowedUp",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_eventStatus":
                  insert_row.push(json_key(lebsResponseForm, "eventStatus", j));
                  break;
                case "lebs_responseForm_humansQuarantinedSelf":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansQuarantinedSelf", j)
                  );
                  break;
                case "lebs_responseForm_humansQuarantinedSchool":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansQuarantinedSchool", j)
                  );
                  break;
                case "lebs_responseForm_humansQuarantinedInstitutional":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "humansQuarantinedInstitutional",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_humansIsolationSchool":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansIsolationSchool", j)
                  );
                  break;
                case "lebs_responseForm_humansIsolationHealthFacility":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "humansIsolationHealthFacility",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_humansIsolationHome":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansIsolationHome", j)
                  );
                  break;
                case "lebs_responseForm_humansIsolationInstitutional":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "humansIsolationInstitutional",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_humansDead":
                  insert_row.push(json_key(lebsResponseForm, "humansDead", j));
                  break;
                case "lebs_responseForm_humansPositive":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansPositive", j)
                  );
                  break;
                case "lebs_responseForm_humansTested":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansTested", j)
                  );
                  break;
                case "lebs_responseForm_humansCases":
                  insert_row.push(json_key(lebsResponseForm, "humansCases", j));
                  break;
                case "lebs_responseForm_humansQuarantined":
                  insert_row.push(
                    json_key(lebsResponseForm, "humansQuarantined", j)
                  );
                  break;
                case "lebs_responseForm_quarantineTypes":
                  insert_row.push(
                    json_key(lebsResponseForm, "quarantineTypes", j)
                  );
                  break;
                case "lebs_responseForm_isHumansIsolated":
                  insert_row.push(
                    json_key(lebsResponseForm, "isHumansIsolated", j)
                  );
                  break;
                case "lebs_responseForm_dateOfTestResults":
                  insert_row.push(
                    json_key(lebsResponseForm, "dateOfTestResults", j)
                  );
                  break;
                case "lebs_responseForm_isolationTypes":
                  insert_row.push(
                    json_key(lebsResponseForm, "isolationTypes", j)
                  );
                  break;
                case "lebs_responseForm_eventStatuses":
                  insert_row.push(
                    json_key(lebsResponseForm, "eventStatuses", j)
                  );
                  break;
                case "lebs_responseForm_responseActivities":
                  insert_row.push(
                    json_key(lebsResponseForm, "responseActivities", j)
                  );
                  break;
                case "lebs_responseForm_additionalResponseActivities":
                  insert_row.push(
                    json_key(
                      lebsResponseForm,
                      "additionalResponseActivities",
                      j
                    )
                  );
                  break;
                case "lebs_responseForm_reasonsNoSampleCollected":
                  insert_row.push(
                    json_key(lebsResponseForm, "reasonsNoSampleCollected", j)
                  );
                  break;

                case "lebs_responseForm_additionalInformation":
                  insert_row.push(
                    json_key(lebsResponseForm, "additionalInformation", j)
                  );
                  break;
                case "lebs_responseForm_via":
                  insert_row.push(json_key(lebsResponseForm, "via", j));
                  break;
                case "lebs_responseForm_createdAt":
                  insert_row.push(json_key(lebsResponseForm, "createdAt", j));
                  break;
                case "lebs_responseForm_updatedAt":
                  insert_row.push(json_key(lebsResponseForm, "updatedAt", j));
                  break;

                //LEBS Escalation Form
                case "lebs_escalationForm_id":
                  insert_row.push(json_key(lebsEscalationForm, "_id", j));
                  break;
                case "lebs_escalationForm_user":
                  insert_row.push(json_key(lebsEscalationForm, "user", j));
                  break;
                case "lebs_escalationForm_dateResponseStarted":
                  insert_row.push(
                    json_key(lebsEscalationForm, "dateResponseStarted", j)
                  );
                  break;
                case "lebs_escalationForm_dateEscalated":
                  insert_row.push(
                    json_key(lebsEscalationForm, "dateEscalated", j)
                  );
                  break;
                case "lebs_escalationForm_reason":
                  insert_row.push(json_key(lebsEscalationForm, "reason", j));
                  break;
                case "lebs_escalationForm_reasonOther":
                  insert_row.push(
                    json_key(lebsEscalationForm, "reasonOther", j)
                  );
                  break;
                case "lebs_escalationForm_via":
                  insert_row.push(json_key(lebsEscalationForm, "via", j));
                  break;

                case "lebs_escalationForm_createdAt":
                  insert_row.push(json_key(lebsEscalationForm, "createdAt", j));
                  break;
                case "lebs_escalationForm_updatedAt":
                  insert_row.push(json_key(lebsEscalationForm, "updatedAt", j));
                  break;

                default:
                  insert_row.push(json_key(data_row, columns[j], j));
              }
              // -------------------------------------------------------
            }
          } catch (e) {
            console.log(e);
          }
          rows.push(insert_row);
        }

        found = mongo_data.length;
        count += found;
        console.log("COUNT:" + count);
        console.log("FOUND:" + found);
        console.log("ROWS:" + rows.length);

        for (r in rows) {
          try {
            let values = rows[r];
            if (
              (queryType == "new_data" && created_at_flag) ||
              queryType == "all_data"
            ) {
              await PostgresConnection().query(pgInsertStatement, values);
            } else if (queryType == "existing_data" && updated_at_flag) {
              await PostgresConnection().query(pgUpdateStatement, values);
            } else {
              console.log("No query type");
            }
          } catch (err) {
            console.log(err.stack);
          }
        }
        console.log(table_name + " data copied successfully");
      }
      console.log(cMessage);
    }
    cb();
    //})
  }
  // -----------------------------

  async.forEachLimit(
    models_index,
    1,
    function (m, modelcb) {
      waterfall(
        [
          async.apply(model_transform, m),
          pgGenerate,
          pgExtract,
          mongoConnect,
          async.apply(startMongoExtract, "all_data", "All Data Inserted"),
          async.apply(startMongoExtract, "new_data", "New Data Inserted"),
          async.apply(
            startMongoExtract,
            "existing_data",
            "Updated Data Inserted"
          ),
          function (cb) {
            cb();
          },
        ],
        function (err, result) {
          console.log("model " + m + " complete");
          modelcb();
        }
      );
    },
    function (err) {
      console.log(err);
      console.log("complete");
      MigrationIsRunning = false;
      if (typeof complete === "function") setTimeout(complete, 1000);
    }
  );
};
