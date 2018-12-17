'use strict';

const db = require('../db/db');

const GTFSTableNames = {
    agency: "agency",
    stops: "stops",
    stoptimes: "stoptimes",
    shapes: "shapes",
    trips: "trips",
    calendardates: "calendardates",
    calendar: "calendar",
    routes: "routes"
};

const GTFSCSVFileNames = {
    agency: "agency.txt",
    stops: "stops.txt",
    stoptimes: "stop_times.txt",
    shapes: "shapes.txt",
    trips: "trips.txt",
    calendardates: "calendar_dates.txt",
    calendar: "calendar.txt",
    routes: "routes.txt"
};

const GTFSCSVFileRequired = {
    agency: true,
    stops: true,
    stoptimes: true,
    shapes: false,
    trips: true,
    calendardates: false,
    calendar: false,
    routes: true
};

const GTFSImportOrder = ["agency", "routes", "stops", "stoptimes", "trips", "shapes", "calendardates", "calendar"];

const idSqlType = "varchar(50)";
const dateSqlType = "TIMESTAMP";
const numericIdSqlType = "int unsigned";
const encryptedSqlType = "char(76)";

const getDayFlag = (dayFlagStr) => { return !!dayFlagStr && dayFlagStr === "1" ? "1" : "0"; };

const agencyGTFSModel = {
    "id": { sqlType: idSqlType, required: false, type: "string", csvFields: ["agency_id"], csvConversion: csvLine => { return `${csvLine.agency_id}`; } },
    "name": { sqlType: "varchar(250)", uniqueKey: true, required: true, type: "string", csvFields: ["agency_name"], csvConversion: csvLine => { return `${csvLine.agency_name}`; } },
    "url": { sqlType: "varchar(250)", required: true, type: "string", csvFields: ["agency_url"], csvConversion: csvLine => { return `${csvLine.agency_url}`; } },
    "timezone": { sqlType: "varchar(50)", required: true, type: "string", csvFields: ["agency_timezone"], csvConversion: csvLine => { return `${csvLine.agency_timezone}`; } },
    "lang": { sqlType: "varchar(2)", required: false, type: "string", csvFields: ["agency_lang"], csvConversion: csvLine => { return `${csvLine.agency_lang}`; } },
    "phone": { sqlType: "varchar(20)", required: false, type: "string", csvFields: ["agency_phone"], csvConversion: csvLine => { return `${csvLine.agency_phone}`; } },
    "fare_url": { sqlType: "varchar(250)", required: false, type: "string", csvFields: ["agency_fare_url"], csvConversion: csvLine => { return `${csvLine.agency_fare_url}`; } },
    "email": { sqlType: "varchar(250)", required: false, type: "string", csvFields: ["agency_email"], csvConversion: csvLine => { return `${csvLine.agency_email}`; } }
};

const stopsGTFSModel = {
    "id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["stop_id"], csvConversion: csvLine => { return `${csvLine.stop_id}`; } },
    "name": { sqlType: "varchar(250)", required: true, type: "string", csvFields: ["stop_name"], csvConversion: csvLine => { return `${csvLine.stop_name}`; } },
    "loc": { sqlType: "point", required: true, type: "geometry", csvFields: ["stop_lon", "stop_lat"], csvConversion: csvLine => { return db.getPointFrom(csvLine.stop_lon, csvLine.stop_lat); } }
};

const stopTimesGTFSModel = {
    "trip_id": { sqlType: idSqlType, primaryKey: true, type: "string", csvFields: ["stop_id"], csvConversion: csvLine => { return `${csvLine.trip_id}`; } },
    "sequence": { sqlType: "INT", primaryKey: true, type: "number", csvFields: ["stop_sequence"], csvConversion: csvLine => { return csvLine.stop_sequence; } },
    "id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["stop_id"], csvConversion: csvLine => { return `${csvLine.stop_id}`; } },
    "arrival_time": { sqlType: dateSqlType, required: true, type: "Date", csvFields: ["arrival_time"], csvConversion: csvLine => { return new Date(csvLine.arrival_time); } },
    "departure_time": { sqlType: dateSqlType, required: true, type: "Date", csvFields: ["departure_time"], csvConversion: csvLine => { return new Date(csvLine.departure_time); } }
};

const shapesGTFSModel = {
    "id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["shape_id"], csvConversion: csvLine => { return `${csvLine.shape_id}`; } },
    "sequence": { sqlType: "INT", primaryKey: true, type: "number", csvFields: ["shape_pt_sequence"], csvConversion: csvLine => { return csvLine.shape_pt_sequence; } },
    "loc": { sqlType: "point", required: true, type: "geometry", csvFields: ["shape_pt_lon", "shape_pt_lat"], csvConversion: csvLine => { return db.getPointFrom(csvLine.shape_pt_lon, csvLine.shape_pt_lat); } }
};

const tripsGTFSModel = {
    "service_id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["service_id"], csvConversion: csvLine => { return `${csvLine.service_id}`; } },
    "id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["trip_id"], csvConversion: csvLine => { return `${csvLine.trip_id}`; } },
    "route_id": { sqlType: idSqlType, required: true, type: "string", csvFields: ["route_id"], csvConversion: csvLine => { return csvLine.route_id; } },
    "direction_id": { sqlType: idSqlType, required: true, type: "string", csvFields: ["direction_id"], csvConversion: csvLine => { return csvLine.direction_id; } },
    "block_id": { sqlType: idSqlType, required: true, type: "string", csvFields: ["block_id"], csvConversion: csvLine => { return csvLine.block_id; } },
    "shape_id": { sqlType: idSqlType, required: true, type: "string", csvFields: ["shape_id"], csvConversion: csvLine => { return csvLine.shape_id; } },
    "headsign": { sqlType: "varchar(250)", required: true, type: "string", csvFields: ["trip_headsign"], csvConversion: csvLine => { return `${csvLine.trip_headsign}`; } }
};

const calendarDatesGTFSModel = {
    "service_id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["service_id"], csvConversion: csvLine => { return `${csvLine.service_id}`; } },
    "date": { sqlType: dateSqlType, primaryKey: true, required: true, type: "Date", csvFields: ["date"], csvConversion: csvLine => { return db.YYYYMMDDToDate(csvLine.date); } },
    "exception_type": { sqlType: "INT", required: true, type: "number", csvFields: ["exception_type"], csvConversion: csvLine => { return `${csvLine.exception_type}`; } }
};

const calendarGTFSModel = {
    "service_id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["service_id"], csvConversion: csvLine => { return `${csvLine.service_id}`; } },
    "start_date": { sqlType: dateSqlType, required: true, type: "Date", csvFields: ["start_date"], csvConversion: csvLine => { return db.YYYYMMDDToDate(csvLine.start_date); } },
    "end_date": { sqlType: dateSqlType, required: true, type: "Date", csvFields: ["end_date"], csvConversion: csvLine => { return db.YYYYMMDDToDate(csvLine.end_date); } },
    "monday": { sqlType: "INT", required: true, type: "number", csvFields: ["monday"], csvConversion: csvLine => { return getDayFlag(csvLine.monday); } },
    "tuesday": { sqlType: "INT", required: true, type: "number", csvFields: ["tuesday"], csvConversion: csvLine => { return getDayFlag(csvLine.tuesday); } },
    "wednesday": { sqlType: "INT", required: true, type: "number", csvFields: ["wednesday"], csvConversion: csvLine => { return getDayFlag(csvLine.wednesday); } },
    "thursday": { sqlType: "INT", required: true, type: "number", csvFields: ["thursday"], csvConversion: csvLine => { return getDayFlag(csvLine.thursday); } },
    "friday": { sqlType: "INT", required: true, type: "number", csvFields: ["friday"], csvConversion: csvLine => { return getDayFlag(csvLine.friday); } },
    "saturday": { sqlType: "INT", required: true, type: "number", csvFields: ["saturday"], csvConversion: csvLine => { return getDayFlag(csvLine.saturday); } },
    "sunday": { sqlType: "INT", required: true, type: "number", csvFields: ["sunday"], csvConversion: csvLine => { return getDayFlag(csvLine.sunday); } }
};

const routesGTFSModel = {
    "id": { sqlType: idSqlType, primaryKey: true, required: true, type: "string", csvFields: ["route_id"], csvConversion: csvLine => { return `${csvLine.route_id}`; } },
    "lname": { sqlType: "varchar(250)", required: true, type: "string", csvFields: ["route_long_name"], csvConversion: csvLine => { return `${csvLine.route_long_name}`; } },
    "sname": { sqlType: "varchar(50)", required: true, type: "string", csvFields: ["route_short_name"], csvConversion: csvLine => { return `${csvLine.route_short_name}`; } },
    "color": { sqlType: "varchar(10)", required: true, type: "string", csvFields: [/*"route_color"*/], csvConversion: csvLine => { return db.getStringFrom(csvLine.route_color, "404040"); } },
    "tcolor": { sqlType: "varchar(10)", required: true, type: "string", csvFields: [/*"route_text_color"*/], csvConversion: csvLine => { return db.getStringFrom(csvLine.route_text_color, "FFFFFF"); } }
};

const GTFSModels = {
    agency: agencyGTFSModel,
    stops: stopsGTFSModel,
    stoptimes: stopTimesGTFSModel,
    shapes: shapesGTFSModel,
    trips: tripsGTFSModel,
    calendardates: calendarDatesGTFSModel,
    calendar: calendarGTFSModel,
    routes: routesGTFSModel
};

const getGTFSImportSpec = (specName, tableNamePrefix, forceCreate) => {
    let model = GTFSModels[specName];
    return {
        csvFileName: GTFSCSVFileNames[specName],
        tableSpecs: db.modelToTableSpecs(tableNamePrefix + GTFSTableNames[specName], model, forceCreate),
        model: model,
        required: GTFSCSVFileRequired[specName]
    };
};

const getGTFSImportSpecs = (tableNamePrefix, forceCreate) => {
    let specs = [];

    tableNamePrefix = tableNamePrefix || "";
    forceCreate = !!forceCreate;

    for (var i in GTFSImportOrder) {
        let specName = GTFSImportOrder[i];
        specs.push(getGTFSImportSpec(specName, tableNamePrefix, forceCreate));
    }

    //specs.push(getGTFSImportSpec("agency", tableNamePrefix, forceCreate));

    return specs;
};

const getGTFSModels = () => { return GTFSModels; };
const getGTFSTableNames = () => { return GTFSTableNames; };
const getGTFSCSVFileNames = () => { return GTFSCSVFileNames; };
const getGTFSCSVFileRequired = () => { return GTFSCSVFileRequired; };

const usersTableName = "users";

const getUserTableName = () => { return usersTableName; };

const getUserTableSpecs = (defaultUserType, forceCreate) => {
    let model = {
        id: { sqlType: numericIdSqlType, autoIncrement: true, primaryKey: true, required: true, type: "number" },
        email: { sqlType: "varchar(100)", uniqueKey: true, required: true, type: "string" },
        password: { sqlType: encryptedSqlType, required: true, type: "string" },
        type: { sqlType: numericIdSqlType, required: false, default: "'" + defaultUserType + "'", type: "number" },
        is_email_confirmed: { sqlType: "tinyint(1)", required: false, default: "'0'", type: "number" },
        last_email_code: { sqlType: encryptedSqlType, required: false, default: "NULL", type: "string" },
        last_email_code_sent: { sqlType: dateSqlType + " NULL", required: false, type: "Date" }
    };
    return db.modelToTableSpecs(usersTableName, model, forceCreate);
};

const agenciesTableName = "agencies";
const agencyKeySqlType = "varchar(8)";

const getAgenciesTableName = () => { return agenciesTableName; };

const getAgencyTableSpecs = forceCreate => {
    let model = {
        id: { sqlType: numericIdSqlType, autoIncrement: true, primaryKey: true, required: true, type: "number" },
        prefix: { sqlType: agencyKeySqlType, uniqueKey: true, required: true, type: "string" }
    };
    return db.modelToTableSpecs(agenciesTableName, model, forceCreate);
};

const userAgencyTableName = "user_agency";
const getUserAgencyTableName = () => { return userAgencyTableName; };

const getUserAgencyTableSpecs = forceCreate => {
    let model = {
        user_id: { sqlType: numericIdSqlType, primaryKey: true, required: true, type: "number", foreignKey: { tableName: usersTableName, columnName: 'id', deleteCascade: true, updateCascade: true } },
        agency_id: { sqlType: numericIdSqlType, primaryKey: true, index: true, required: true, type: "number", foreignKey: { tableName: agenciesTableName, columnName: 'id', deleteCascade: true, updateCascade: true } }
        //agency_id: { sqlType: agencyKeySqlType, primaryKey: true, index: true, required: true, type: "string", foreignKey: { tableName: agenciesTableName, columnName: 'id', deleteCascade: true, updateCascade: true } }
    };
    return db.modelToTableSpecs(userAgencyTableName, model, forceCreate);
};

module.exports = {
    getGTFSTableNames: getGTFSTableNames,
    getGTFSCSVFileNames: getGTFSCSVFileNames,
    getGTFSCSVFileRequired: getGTFSCSVFileRequired,
    getGTFSModels: getGTFSModels,
    getGTFSImportSpecs: getGTFSImportSpecs,
    getUserTableSpecs: getUserTableSpecs,
    getUserTableName: getUserTableName,
    getAgencyTableSpecs: getAgencyTableSpecs,
    getAgenciesTableName: getAgenciesTableName,
    getUserAgencyTableName: getUserAgencyTableName,
    getUserAgencyTableSpecs: getUserAgencyTableSpecs
};
