'use strict';

const Promise = require("bluebird");
const promiseMySQL = require('promise-mysql');
const moment = require('moment');
//const mySQL = require('mysql');

let dbName = 'Transit';
let dbConnSettings;
let promisePool;
//let pool;

const countStar = "count(*)";

//const createConnection = (cb) => { let con = mySQL.createConnection(dbConnSettings); con.connect(err => { cb(err, con); }); };

//const getPoolConnection = (cb) => { pool.getConnection(cb); }

const checkCreatePromisePool = () => { return promisePool ? promisePool : promiseMySQL.createPool(dbConnSettings); };

const getSqlConnection = () => {
    return promisePool ? promisePool.getConnection().disposer((connection) => {
        promisePool.releaseConnection(connection);
    }) : undefined;
};

const endConnection = (conn) => { if (conn && conn.end) { conn.end(); } return undefined; };

const queryConnVoid = async (conn, sqlStr) => { return conn.query(sqlStr); };

const queryConnValues = async (conn, sqlStr, values) => {
    return conn.query(sqlStr, values);
};

const queryPoolVoid = async (sqlStr) => { return Promise.using(getSqlConnection(), (conn) => { return queryConnVoid(conn, sqlStr); }); };

const queryPoolValues = async (sqlStr, values) => { return Promise.using(getSqlConnection(), (conn) => { return queryConnValues(conn, sqlStr, values); }); };

const checkCreateDatabase = async () => {
    let connection;
    return promiseMySQL.createConnection(
        dbConnSettings
    ).then(conn => {
        connection = conn;
        return queryConnVoid(conn, "CREATE DATABASE IF NOT EXISTS " + dbName + ";");
    }).then(() => {
        connection = endConnection(connection);
    }).catch(error => {
        connection = endConnection(connection);
        throw error;
    });
};

const init = async (settings) => {
    if (settings) {
        dbName = settings.dbName || dbName;
        dbConnSettings = Object.assign({}, settings.connSettings);
    }
    return checkCreateDatabase().then(() => {
        dbConnSettings.database = dbName;
        return checkCreatePromisePool();
    }).then(createdPool => {
        if (createdPool) {
            promisePool = createdPool;
            //pool = mySQL.createPool(dbConnSettings);
        } else { throw new Error('failed to create db connection promisePool'); }
    });
};

const makeRemoveTableConstraintSql = (tableName, fieldName) => { return 'alter table `' + tableName + '` drop foreign key `ct_' + tableName + '_' + fieldName + '`;'; };

const removeTableConstraint = async (tableName, fieldName) => { return queryPoolVoid(makeRemoveTableConstraintSql(tableName, fieldName)); };

const makeForeignKeyConstraintSql = (tableName, fieldName, foreignKeyTableName, foreignKeyColumnName) => {
    return 'CONSTRAINT ' + '`ct_' + tableName + '_' + fieldName + '`' + ' FOREIGN KEY ' + '`fk_' + fieldName + '` (`' + fieldName + '`) REFERENCES `' + foreignKeyTableName + '`(`' + foreignKeyColumnName + '`)';
};

const makeAddTableForeignKeyConstraintSql = (tableName, fieldName, foreignKeyTableName, foreignKeyColumnName) => {
    let sql = makeForeignKeyConstraintSql(tableName, fieldName, foreignKeyTableName, foreignKeyColumnName);
    return 'alter table `' + tableName + '` add ' + sql + ';';
};

const addTableForeignKeyConstraint = async (tableName, fieldName, foreignKeyTableName, foreignKeyColumnName) => {
    //console.log(makeAddTableForeignKeyConstraintSql(tableName, fieldName, foreignKeyTableName, foreignKeyColumnName));
    return queryPoolVoid(makeAddTableForeignKeyConstraintSql(tableName, fieldName, foreignKeyTableName, foreignKeyColumnName));
};

const makeCreateTableSql = (tableSpecs, resolveForeignKeyTableName) => {
    let createCommand = "CREATE TABLE";
    if (!tableSpecs.forceCreate) { createCommand += " IF NOT EXISTS"; }
    let sqlCreateTable = createCommand + " `" + tableSpecs.tableName + "` (";
    let nFields = tableSpecs.fields.length;
    let primaryKeyStr = "", uniqueKeyStr = "", indexStr = "";
    let foreignKeyStr = "";
    for (let i = 0; i < nFields; ++i) {
        let field = tableSpecs.fields[i];
        let sqlFieldName = "`" + field.name + "`";
        let sqlField = sqlFieldName + " " + field.sqlType;
        if (field.required) { sqlField += " NOT NULL"; }
        if (field.default !== undefined) { sqlField += " DEFAULT " + field.default; }
        if (field.autoIncrement) { sqlField += " AUTO_INCREMENT"; }
        if (i < nFields - 1) {
            sqlField += ',';
        }
        sqlCreateTable += sqlField;
        if (field.primaryKey) {
            if (primaryKeyStr.length) { primaryKeyStr += ','; }
            primaryKeyStr += sqlFieldName;
        }
        if (field.uniqueKey) {
            if (uniqueKeyStr.length) { uniqueKeyStr += ','; }
            uniqueKeyStr += sqlFieldName;
        }
        if (field.index) {
            if (indexStr.length) { indexStr += ','; }
            indexStr += sqlFieldName;
        }
        if (field.foreignKey) {
            if (foreignKeyStr.length) { foreignKeyStr += ','; }
            let foreignKeyTableName = resolveForeignKeyTableName !== undefined ? resolveForeignKeyTableName(field.foreignKey.tableName) : field.foreignKey.tableName;
            //let thisForeignKeyStr = ' CONSTRAINT ' + '`ct_' + tableSpecs.tableName + '_' + field.name + '`' + ' FOREIGN KEY ' + '`fk_' + field.name + '` (' + sqlFieldName + ') REFERENCES `' + foreignKeyTableName + '`(`' + field.foreignKey.columnName + '`)';
            let thisForeignKeyStr = ' ' + makeForeignKeyConstraintSql(tableSpecs.tableName, field.name, foreignKeyTableName, field.foreignKey.columnName);
            if (field.foreignKey.deleteCascade) { thisForeignKeyStr += " ON DELETE CASCADE"; }
            if (field.foreignKey.updateCascade) { thisForeignKeyStr += " ON UPDATE CASCADE"; }
            foreignKeyStr += thisForeignKeyStr;
        }
    }
    if (primaryKeyStr.length > 0) { sqlCreateTable += ",PRIMARY KEY (" + primaryKeyStr + ")"; }
    if (uniqueKeyStr.length > 0) { sqlCreateTable += ",UNIQUE KEY (" + uniqueKeyStr + ")"; }
    if (indexStr.length > 0) { sqlCreateTable += ",INDEX (" + indexStr + ")"; }
    if (foreignKeyStr.length > 0) { sqlCreateTable += "," + foreignKeyStr; }
    sqlCreateTable += ") ENGINE=";
    let engineStr = tableSpecs.engine !== undefined ? tableSpecs.engine : "InnoDB";
    sqlCreateTable += engineStr;
    sqlCreateTable += " DEFAULT CHARSET=utf8;";
    return sqlCreateTable;
};

const makeCreateTableSql2 = (tableSpecs, resolveForeignKeyTableName) => {
    let createCommand = "CREATE TABLE";
    if (!tableSpecs.forceCreate) { createCommand += " IF NOT EXISTS"; }
    let sqlCreateTable = createCommand + " `" + tableSpecs.tableName + "` (";
    let engineStr = tableSpecs.engine !== undefined ? tableSpecs.engine : "InnoDB";
    let nFields = tableSpecs.fields.length;
    let primaryKeyStr = "";
    let uniqueKeyStrs = {};
    let indexStrs = {};
    let spatialIndices = [];
    let foreignKeyStr = "";
    for (let i = 0; i < nFields; ++i) {
        let field = tableSpecs.fields[i];
        let sqlFieldName = "`" + field.name + "`";
        let sqlField = sqlFieldName + " " + field.sqlType;

        if (field.required) { sqlField += " NOT NULL"; }
        if (field.default !== undefined) { sqlField += " DEFAULT " + field.default; }
        if (field.autoIncrement) { sqlField += " AUTO_INCREMENT"; }
        if (i < nFields - 1) { sqlField += ','; }

        sqlCreateTable += sqlField;

        if (field.primaryKey) { if (primaryKeyStr.length) { primaryKeyStr += ','; } primaryKeyStr += sqlFieldName; }

        if (field.uniqueKey !== undefined && field.uniqueKey !== false) {
            let str = uniqueKeyStrs[field.uniqueKey], isFirstField = str === undefined;
            if (isFirstField) { str = ""; } else { str += ","; }
            str += sqlFieldName;
            uniqueKeyStrs[field.uniqueKey] = str;
        }

        if (field.index !== undefined && field.index !== false) {
            let str = indexStrs[field.index], isFirstField = str === undefined;
            if (isFirstField) { str = ""; } else { str += ","; }
            str += sqlFieldName;
            indexStrs[field.index] = str;
        }

        if (field.spatialIndex !== undefined && field.index !== false) { spatialIndices.push(sqlFieldName); }

        if (field.foreignKey) {
            if (foreignKeyStr.length) { foreignKeyStr += ','; }
            let foreignKeyTableName = resolveForeignKeyTableName !== undefined ? resolveForeignKeyTableName(field.foreignKey.tableName) : field.foreignKey.tableName;
            let thisForeignKeyStr = ' ' + makeForeignKeyConstraintSql(tableSpecs.tableName, field.name, foreignKeyTableName, field.foreignKey.columnName);
            if (field.foreignKey.deleteCascade) { thisForeignKeyStr += " ON DELETE CASCADE"; }
            if (field.foreignKey.updateCascade) { thisForeignKeyStr += " ON UPDATE CASCADE"; }
            foreignKeyStr += thisForeignKeyStr;
        }
    }

    if (primaryKeyStr.length > 0) { sqlCreateTable += ",PRIMARY KEY (" + primaryKeyStr + ")"; }

    for (let i in spatialIndices) { let str = spatialIndices[i]; sqlCreateTable += ",SPATIAL INDEX(" + str + ")"; }

    for (let i in uniqueKeyStrs) { let str = uniqueKeyStrs[i]; sqlCreateTable += ",UNIQUE KEY (" + str + ")"; } 
    for (let i in indexStrs) { let str = indexStrs[i]; sqlCreateTable += ",INDEX (" + str + ")"; }

    if (foreignKeyStr.length > 0) { sqlCreateTable += "," + foreignKeyStr; }

    sqlCreateTable += ") ENGINE=" + engineStr + " DEFAULT CHARSET=utf8;";
    return sqlCreateTable;
};

const checkCreateTable = async (tableSpecs, resolveForeignKeyTableName) => { return queryPoolVoid(makeCreateTableSql(tableSpecs, resolveForeignKeyTableName)); };
const checkCreateTable2 = async (tableSpecs, resolveForeignKeyTableName) => { return queryPoolVoid(makeCreateTableSql2(tableSpecs, resolveForeignKeyTableName)); };

const dropTableIfExists = async (tableName) => {
    let sqlStr = "DROP TABLE IF EXISTS " + tableName;
    //console.log(`dropping table if exists: ${tableName}`);
    return queryPoolVoid(sqlStr);
};

const select = async (tableName, fieldNames, whereStr, orderByStr) => {
    let sqlStr = "SELECT ";
    let nFieldNames = fieldNames.length;
    for (let i = 0; i < nFieldNames; ++i) {
        if (i > 0) { sqlStr += ','; }
        sqlStr += fieldNames[i];
    }
    sqlStr += " FROM " + tableName;
    if (whereStr !== undefined) { sqlStr += " WHERE " + whereStr; }
    if (orderByStr !== undefined) { sqlStr += " ORDER BY " + orderByStr; }
    sqlStr += ";";
    return queryPoolVoid(sqlStr);
};

const insert = async (tableName, fieldNames, values) => {
    let sqlStr = "INSERT INTO " + tableName + "(";
    let nFieldNames = fieldNames.length;
    for (let i = 0; i < nFieldNames; ++i) {
        if (i > 0) { sqlStr += ','; }
        sqlStr += "`" + fieldNames[i] + "`";
    }
    sqlStr += ") VALUES ? ON DUPLICATE KEY UPDATE ";
    for (let i = 0; i < nFieldNames; ++i) {
        let fn = fieldNames[i];
        if (i > 0) { sqlStr += ','; }
        sqlStr += "`" + fn + "`" + "=VALUES(`" + fn + "`)";
    }
    sqlStr += ";";
    //console.log(sqlStr);
    return queryPoolValues(sqlStr, values);
};

const insertNoDuplicateKey = async (tableName, fieldNames, values) => {
    let sqlStr = "INSERT INTO " + tableName + "(";
    let nFieldNames = fieldNames.length;
    for (let i = 0; i < nFieldNames; ++i) {
        if (i > 0) { sqlStr += ','; }
        sqlStr += "`" + fieldNames[i] + "`";
    }
    sqlStr += ") VALUES ? ;";
    //console.log(sqlStr);
    return queryPoolValues(sqlStr, values);
};

const update = async (tableName, values, useWhere) => {
    let sqlStr = "UPDATE " + tableName + " SET ?";
    if (useWhere) { sqlStr += " WHERE ?"; }
    sqlStr += ";";
    //console.log(sqlStr);
    return queryPoolValues(sqlStr, values);
};

const deleteQuery = async (tableName, whereStr, orderByStr) => {
    let sqlStr = "DELETE FROM " + tableName;
    if (whereStr !== undefined) { sqlStr += " WHERE " + whereStr; }
    if (orderByStr !== undefined) { sqlStr += " ORDER BY " + orderByStr; }
    sqlStr += ";";
    //console.log(sqlStr);
    return queryPoolValues(sqlStr);
};

const getTableCount = async (tableName, whereStr) => {
    return select(tableName, [countStar], whereStr)
        .then(results => {
            return results.length === 1 ? results[0][countStar] : 0;
        });
};

const modelToTableSpecs = (tableName, model, forceCreate) => {
    let fields = [], fieldNames = [], nonAIFieldNames = [];
    for (let i in model) {
        let f = model[i];
        fields.push(Object.assign({ name: i }, f));
        fieldNames.push(i);
        if (!f.autoIncrement) { nonAIFieldNames.push(i); }
    }
    return { tableName: tableName, fieldNames: fieldNames, nonAIFieldNames: nonAIFieldNames, fields: fields, engine: 'InnoDB', model: model, forceCreate: forceCreate };
};

const getFirstResult = (results) => { return !!results && results.length > 0 ? results[0] : undefined; };

const getPointFrom = (lon, lat) => { return { geom: "POINT", coords: [lon, lat] }; };
const getStringFrom = (colorName, defaultColor) => { return colorName ? colorName : defaultColor; };

const YYYYMMDDFormat = 'YYYYMMDD';

const YYYYMMDDToDate = (dateStr) => { return moment(dateStr, YYYYMMDDFormat).toDate(); };
const YYYYMMDDToString = (dateStr) => { return moment(dateStr, YYYYMMDDFormat).format(YYYYMMDDFormat); };

let minEmailLength = 5;
const emailRE = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;

const validateEmail = email => {
    let ok = false, message = "a valid email address is required";
    if (!!email && email.length >= minEmailLength) {
        if ((ok = emailRE.test(email)) === true) {
            message = "email valid";
        }
    }
    return { ok: ok, message: message };
};

const minPasswordLength = 8;
const upperCaseRE = /[A-Z]/, lowerCaseRE = /[a-z]/, numbersRE = /\d/, nonAlphasRE = /\W/;

const validatePassword = password => {
    let ok = false, message = "";
    if (!!password && password.length >= minPasswordLength) {
        let hasUpperCase = upperCaseRE.test(password);
        let hasLowerCase = lowerCaseRE.test(password);
        let hasNumbers = numbersRE.test(password);
        let hasNonAlphas = nonAlphasRE.test(password);
        if ((ok = hasUpperCase + hasLowerCase + hasNumbers + hasNonAlphas >= 3) === true) {
            message = "password valid";
        }
        else {
            message = "password must contain upper and lower case <br />letters and at least one symbol or digit";
        }
    }
    else { message = "password must contain at least " + minPasswordLength + " characters"; }
    return { ok: ok, message: message };
};

const checkIfTableExists = async tableName => {
    let sql = 'SELECT 1 FROM `' + tableName + '` LIMIT 1';
    return queryPoolVoid(sql).then(result => {
        return true;
    }).catch(err => {
        //console.log('checkIfTableExists: ' + err);
        return false;
    });
};

module.exports = {
    init: init,
    //createConnection: createConnection,
    //getPoolConnection: getPoolConnection,
    getSqlConnection: getSqlConnection,
    queryPoolVoid: queryPoolVoid,
    queryPoolValues: queryPoolValues,
    dropTableIfExists: dropTableIfExists,
    makeCreateTableSql: makeCreateTableSql,
    checkIfTableExists: checkIfTableExists,
    checkCreateTable: checkCreateTable,
    checkCreateTable2: checkCreateTable2,
    makeRemoveTableConstraintSql: makeRemoveTableConstraintSql,
    removeTableConstraint: removeTableConstraint,
    makeAddTableForeignKeyConstraintSql: makeAddTableForeignKeyConstraintSql,
    addTableForeignKeyConstraint: addTableForeignKeyConstraint,
    select: select,
    insert: insert,
    insertNoDuplicateKey: insertNoDuplicateKey,
    update: update,
    deleteQuery: deleteQuery,
    getTableCount: getTableCount,
    modelToTableSpecs,
    getFirstResult: getFirstResult,
    getPointFrom: getPointFrom,
    getStringFrom: getStringFrom,
    YYYYMMDDToDate: YYYYMMDDToDate,
    YYYYMMDDToString: YYYYMMDDToString,
    validateEmail: validateEmail,
    validatePassword: validatePassword
};
