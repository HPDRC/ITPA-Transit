'use strict';

const path = require('path');
const Promise = require("bluebird");
const fs = require('fs-extra');
const util = require('util');
const stream = require('stream');
const db = require('../db/db');
const geom = require('../lib/geom');
const csvio = require('../db/csvio');

let agency;
const getAgency = () => { if (agency === undefined) { agency = require('../db/agency'); } return agency; };

//const csvImportSql = "LOAD DATA LOCAL INFILE ? INTO TABLE ?? FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;";

const engineSql = "InnoDB";                 // needed for foreign keys (currently not used) and for transaction support (used), etc. https://en.wikipedia.org/wiki/Comparison_of_MySQL_database_engines

const numericIdSqlType = "int unsigned";
const autoIncIdSqlType = 'INT(12)';
const idSqlType = "varchar(50)";
const longSqlCharType = "varchar(255)";
const shortSqlCharType = "varchar(50)";
const byteSqlType = "tinyint(3) unsigned";
const dateSqlType = "date";
const ddmmyySqlType = "varchar(8)";
const hhmmssSqlType = "varchar(8)";
const hmsSqlTime = 'int unsigned';
const textSqlType = "text";
const countSqlType = 'int unsigned';
//const blobSqlType = "blob";
//const blobSqlType = "text";
const blobSqlType = "longtext";
const lonSqlType = "DECIMAL(9,6)";
const latSqlType = "DECIMAL(8,6)";
const pointSqlType = "POINT";
const sequenceSqlType = "SMALLINT";
const distanceSqlType = "FLOAT";
const directionFriendlySqlType = "varchar(20)";

const autoIncKeyColumnName = 'id';

const gtfs_direction_name_FieldName = 'gtfs_direction_name';

const gtfsDirectionFriendlyNamesArray = ['Northbound', 'Southbound', 'Eastbound', 'Westbound', 'Clockwise', 'CntrClockwise'];
const gtfsDirectionNamesArray = ['northbound', 'southbound', 'eastbound', 'westbound', 'clockwise', 'cntrclockwise'];
const gtfsDirectionNamesMap = gtfsDirectionNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const gtfsDirectionNamesIdArray = gtfsDirectionNamesArray.map((cur, index) => { return { name: cur, index: index }; });
const gtfsDirectionNamesIdMap = gtfsDirectionNamesIdArray.reduce((prev, cur) => { prev[cur.name] = cur.index; return prev; }, {});

const gtfsRouteTypesArray = [
    { name: 'Tram', desc: 'Streetcar, Light rail.Any light rail or street level system within a metropolitan area' },
    { name: 'Subway, Metro', desc: 'Any underground rail system within a metropolitan area' },
    { name: 'Rail', desc: 'Used for intercity or long distance travel' },
    { name: 'Bus', desc: 'Used for short and long distance bus routes' },
    { name: 'Ferry', desc: 'Used for short and long distance boat service' },
    { name: 'Cable car', desc: 'Used for street level cable cars where the cable runs beneath the car' },
    { name: 'Gondola, Suspended cable car', desc: 'Typically used for aerial cable cars where the car is suspended from the cable' },
    { name: 'Funicular', desc: 'Any rail system designed for steep inclines' },
];
const gtfsRouteTypesMap = gtfsRouteTypesArray.reduce((theMap, cur, index) => { theMap[cur.name] = Object.assign(cur, { value: index }); return theMap;}, {});
const gtfsDefaultRouteTypeIndex = 3;

const gtfsStopLocationTypesArray = [
    { name: 'Stop', desc: 'A location where passengers board or disembark from a transit vehicle' },
    { name: 'Station', desc: 'A physical structure or area that contains one or more stops' }
];
const gtfsStopLocationTypesMap = gtfsStopLocationTypesArray.reduce((theMap, cur, index) => { theMap[cur.name] = Object.assign(cur, { value: index }); return theMap; }, {});
const gtfsStopDefaultLocationType = 0;

const gtfsStopWheelChairBordingArray = [
    { desc: 'There is no accessibility information' },
    { desc: 'At least some vehicles can be boarded by a rider in a wheelchair' },
    { desc: 'Wheelchair boarding is not possible' }
];

const gtfsStopInStationWheelChairBordingArray = [
    { desc: 'Accessibility information available in parent station' },
    { desc: 'There exists some accessible path from outside the station' },
    { desc: 'There exists no accessible path from outside the station' }
];

const gtfsStopDefautWheelChairBoarding = 0;

const gtfsTripWheelChairAccessibleArray = [
    { desc: 'Information about wheelchair accomodations is not provided' },
    { desc: 'Accomodations are available for at least one rider in a wheelchair' },
    { desc: 'No riders with wheelchairs can be accomodated' }
];

const gtfsTripWheelChairAccesibleDefault = 0;

const gtfsPickupDropTypesArray = [
    { desc: 'Regularly scheduled' },
    { desc: 'Not available' },
    { desc: 'Must arrange with agency' },
    { desc: 'Must coordinate with driver' }
];

const gtfsStopDefautPickupDropOffType = 0;

const gtfsTripBikesAllowedArray = [
    { desc: 'Accomodation information is not provided' },
    { desc: 'At least one bicycle can be accomodated' },
    { desc: 'No bicycles are allowed' }
];

const gtfsTripBikesAllowedDefault = 0;

const gtfsStopTimeTimePointTypesArray = [
    { desc: 'Approximate times' },
    { desc: 'Exact times' }
];
const gtfsDefaultStopTimeTimePointType = 1;

const gtfsCalendarDatesExceptionTypesArray = [
    { desc: 'Service added and available' },
    { desc: 'Service removed and unavailable' }
];

const gtfsCalendarDatesExceptionTypeAvailable = 1;
const gtfsCalendarDatesExceptionTypeUnavailable = 2;

const gtfsCalendarDatesExceptionTypeDefault = gtfsCalendarDatesExceptionTypeAvailable;

const gtfsByDateWDMaskStr = "by date";

const gtfsWeekDayNamesArray = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];
const gtfsDayTwoLetterNames = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];
const gtfsWeekDayNamesMap = gtfsWeekDayNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const gtfsWeekDayNameMasks = gtfsWeekDayNamesArray.reduce((prev, cur, index) => { let mask = 1 << index; prev[cur] = mask; return prev; }, {});

const gtfsFriendlyServiceNameForWeekDayMask = {
    '0': gtfsByDateWDMaskStr, '8': "Thursday", '31': "Weekday", '32': "Saturday", '16': "Friday",
    '48': "Fri&Sat", '64': "Sunday", '79': "Sun-Thu", '63': "ExceptSun", '127': "Everyday"
};

const gtfsGetFriendlyDayMaskName = wdm => {
    let serviceName = gtfsFriendlyServiceNameForWeekDayMask['' + wdm];
    if (!serviceName) {
        let nDays = gtfsDayTwoLetterNames.length;
        serviceName = '';
        for (let i = 0; i < nDays; ++i) {
            let dayMask = 1 << i;
            if (dayMask & wdm) { serviceName += gtfsDayTwoLetterNames[i]; }
        }
    }
    return serviceName;
};

const gtfsGetDayMaskFromCalendarObj = calendarObj => {
    let mask = 0;
    if (calendarObj) {
        let nDays = gtfsWeekDayNamesArray.length;
        for (let i = 0; i < nDays; ++i) {
            let wdn = gtfsWeekDayNamesArray[i];
            let value = calendarObj[wdn];
            if (value === true || value === '1') {
                mask |= gtfsWeekDayNameMasks[wdn];
            }
        }
    }
    return mask;
};

const gtfsGetDayMaskFromDate = (theDate) => {
    let mask = 0;
    if (theDate instanceof Date) {
        let dow = theDate.getDay();
        dow = dow === 0 ? 6 : dow - 1;
        mask = 1 << dow;
    }
    return mask;
};

const lineStringSimplifyTolerance = 4;
const lineStringPrecision = 5;
const distancesPrecision = 4;

const csvFileNamesArray = ['agency', 'routes', 'stops', 'shapes', 'calendar', 'calendar_dates', 'trips', 'stop_times'];
const csvFileNamesMap = csvFileNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});

const agencyFieldNamesArray = [autoIncKeyColumnName, 'agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone', 'agency_fare_url', 'agency_email'];
const agencyFieldNamesMap = agencyFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const agencyFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: agencyFieldNamesMap.agency_id, sqlType: idSqlType, required: false, gtfs: true },
    { name: agencyFieldNamesMap.agency_name, sqlType: longSqlCharType, uniqueKey: true, required: true, gtfs: true },
    { name: agencyFieldNamesMap.agency_url, sqlType: longSqlCharType, required: true, gtfs: true },
    { name: agencyFieldNamesMap.agency_timezone, sqlType: shortSqlCharType, required: true, gtfs: true },
    { name: agencyFieldNamesMap.agency_lang, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: agencyFieldNamesMap.agency_phone, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: agencyFieldNamesMap.agency_fare_url, sqlType: longSqlCharType, required: false, gtfs: true },
    { name: agencyFieldNamesMap.agency_email, sqlType: longSqlCharType, required: false, gtfs: true }
];
const agencyGTFSFieldNamesArray = agencyFieldNamesArray.filter((t, index) => { return agencyFieldsArray[index].gtfs; });
const agencyModel = agencyFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const routesFieldNamesArray = [autoIncKeyColumnName, 'route_id', 'agency_id', 'agency_id_in_agency', 'route_short_name', 'route_long_name', 'route_desc', 'route_type', 'route_url', 'route_color', 'route_text_color'];
const routesFieldNamesMap = routesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const routesFieldsArray = [
    { name: routesFieldNamesMap.id, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: routesFieldNamesMap.route_id, sqlType: idSqlType, uniqueKey: 1, required: true, gtfs: true },
    { name: routesFieldNamesMap.agency_id, sqlType: autoIncIdSqlType, required: false, index: 1, gtfs: true },
    { name: routesFieldNamesMap.agency_id_in_agency, sqlType: idSqlType, uniqueKey: 1, required: true, gtfs: true },
    { name: routesFieldNamesMap.route_short_name, sqlType: shortSqlCharType, required: false, gtfs: true },     // BCTA - Broward County - does not include route short name
    { name: routesFieldNamesMap.route_long_name, sqlType: longSqlCharType, required: false, gtfs: true },       // BCTA - Broward County - does not include route long name
    { name: routesFieldNamesMap.route_desc, sqlType: textSqlType, required: false, gtfs: true },
    { name: routesFieldNamesMap.route_type, sqlType: byteSqlType, required: true, index: 2, gtfs: true },
    { name: routesFieldNamesMap.route_url, sqlType: longSqlCharType, required: false, gtfs: true },
    { name: routesFieldNamesMap.route_color, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: routesFieldNamesMap.route_text_color, sqlType: shortSqlCharType, required: false, gtfs: true }
];
const routesGTFSFieldNamesArray = routesFieldNamesArray.filter((t, index) => { return routesFieldsArray[index].gtfs; });
const routesModel = routesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const stopsFieldNamesArray = [
    autoIncKeyColumnName, 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'stop_point', 'zone_id', 'stop_url', 'location_type', 'parent_station', 'stop_timezone', 'wheelchair_boarding'];
const stopsFieldNamesMap = stopsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const stopsFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: true },
    { name: stopsFieldNamesMap.stop_id, sqlType: idSqlType, uniqueKey: true, required: true, gtfs: true },
    { name: stopsFieldNamesMap.stop_code, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.stop_name, sqlType: longSqlCharType, required: true, gtfs: true },
    { name: stopsFieldNamesMap.stop_desc, sqlType: textSqlType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.stop_lat, sqlType: latSqlType, required: true, gtfs: true },
    { name: stopsFieldNamesMap.stop_lon, sqlType: lonSqlType, required: true, gtfs: true },
    { name: stopsFieldNamesMap.stop_point, sqlType: pointSqlType, required: true, spatialIndex: true, gtfs: false },
    { name: stopsFieldNamesMap.zone_id, sqlType: idSqlType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.stop_url, sqlType: longSqlCharType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.location_type, sqlType: byteSqlType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.parent_station, sqlType: idSqlType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.stop_timezone, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: stopsFieldNamesMap.wheelchair_boarding, sqlType: byteSqlType, required: false, gtfs: true }
];
const stopsGTFSFieldNamesArray = stopsFieldNamesArray.filter((t, index) => { return stopsFieldsArray[index].gtfs; });
/*const stopsFieldNamesQueryArray = stopsFieldNamesArray.reduce((prev, cur) => {
    if (cur !== stopFieldNamesMap.stop_lat && cur !== stopFieldNamesMap.stop_lon) {
        if (cur === stopFieldNamesMap.stop_lat)
        prev.push(cur);
    }
    return prev;
}, []);*/
const stopsModel = stopsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const calendarFieldNamesArray = [
    autoIncKeyColumnName, 'service_id',
    gtfsWeekDayNamesMap.monday,
    gtfsWeekDayNamesMap.tuesday,
    gtfsWeekDayNamesMap.wednesday,
    gtfsWeekDayNamesMap.thursday,
    gtfsWeekDayNamesMap.friday,
    gtfsWeekDayNamesMap.saturday,
    gtfsWeekDayNamesMap.sunday,
    'wd_mask', 'wd_mask_name', 'start_date', 'end_date', 'start_date_date', 'end_date_date'
];
const calendarFieldNamesMap = calendarFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const calendarFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: calendarFieldNamesMap.service_id, sqlType: idSqlType, uniqueKey: true, required: true, gtfs: true },
    { name: calendarFieldNamesMap.monday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.tuesday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.wednesday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.thursday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.friday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.saturday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.sunday, sqlType: byteSqlType, required: true, gtfs: true },
    { name: calendarFieldNamesMap.wd_mask, sqlType: byteSqlType, required: true, gtfs: false },
    { name: calendarFieldNamesMap.wd_mask_name, sqlType: shortSqlCharType, required: true, gtfs: false },
    { name: calendarFieldNamesMap.start_date, sqlType: ddmmyySqlType, required: false, gtfs: true },
    { name: calendarFieldNamesMap.end_date, sqlType: ddmmyySqlType, required: false, gtfs: true },
    { name: calendarFieldNamesMap.start_date_date, sqlType: dateSqlType, required: false, gtfs: false },
    { name: calendarFieldNamesMap.end_date_date, sqlType: dateSqlType, required: false, gtfs: false }
];
const calendarModel = calendarFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const shapesFieldNamesArray = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence', 'shape_dist_traveled'];
const shapesFieldNamesMap = shapesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const shapesFieldsArray = [
    { name: shapesFieldNamesMap.shape_id, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: shapesFieldNamesMap.shape_pt_lat, sqlType: latSqlType, required: true, gtfs: true },
    { name: shapesFieldNamesMap.shape_pt_lon, sqlType: lonSqlType, required: true, gtfs: true },
    { name: shapesFieldNamesMap.shape_pt_sequence, sqlType: sequenceSqlType, primaryKey: true, required: true, gtfs: true },
    { name: shapesFieldNamesMap.shape_dist_traveled, sqlType: distanceSqlType, required: false, gtfs: true }
];
const shapesModel = shapesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const calendar_datesFieldNamesArray = [autoIncKeyColumnName, 'service_id', 'service_id_in_agency', 'date', 'exception_type', 'date_date'];
const calendar_datesFieldNamesMap = calendar_datesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const calendar_datesFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: calendar_datesFieldNamesMap.service_id, sqlType: autoIncIdSqlType, index: 1, uniqueKey: 1, required: true, gtfs: true },
    { name: calendar_datesFieldNamesMap.service_id_in_agency, sqlType: idSqlType, index: 2, required: true, gtfs: true },
    { name: calendar_datesFieldNamesMap.date, sqlType: ddmmyySqlType, uniqueKey: 1, required: true, gtfs: true },
    { name: calendar_datesFieldNamesMap.exception_type, sqlType: byteSqlType, index: 3, uniqueKey: 1, required: true, gtfs: true },
    { name: calendar_datesFieldNamesMap.date_date, sqlType: dateSqlType, index: 4, required: true, gtfs: false }
];
const calendar_datesModel = calendar_datesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const tripsFieldNamesArray = [
    autoIncKeyColumnName, 'route_id', 'service_id', 'trip_id', 'trip_headsign', 'trip_short_name', 'direction_id', 'block_id', 'shape_id', 'wheelchair_accessible', 'bikes_allowed', 'route_type'
];
const tripsFieldNamesMap = tripsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const tripsFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true, gtfs: false },
    { name: tripsFieldNamesMap.route_id, sqlType: autoIncIdSqlType, required: true, gtfs: true},
    { name: tripsFieldNamesMap.service_id, sqlType: autoIncIdSqlType, required: true, gtfs: true },
    { name: tripsFieldNamesMap.trip_id, sqlType: idSqlType, uniqueKey: 1, required: true, gtfs: true },
    { name: tripsFieldNamesMap.trip_headsign, sqlType: longSqlCharType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.trip_short_name, sqlType: shortSqlCharType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.direction_id, sqlType: byteSqlType, required: false, default: '0', gtfs: true },
    { name: tripsFieldNamesMap.block_id, sqlType: idSqlType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.shape_id, sqlType: autoIncIdSqlType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.wheelchair_accessible, sqlType: byteSqlType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.bikes_allowed, sqlType: byteSqlType, required: false, gtfs: true },
    { name: tripsFieldNamesMap.route_type, sqlType: byteSqlType, required: true, gtfs: false }
];
const tripsModel = tripsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});
const tripsIndices = [
    tripsFieldNamesMap.route_id,
    tripsFieldNamesMap.service_id,
    tripsFieldNamesMap.block_id,
    tripsFieldNamesMap.route_type
];

const stop_timesFieldNamesArray = [
    'trip_id', 'arrival_time', 'departure_time', 'arrival_hms', 'departure_hms', 'stop_id', 'stop_sequence', 'stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint'
];
const stop_timesFieldNamesMap = stop_timesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const stop_timesFieldsArray = [
    { name: stop_timesFieldNamesMap.trip_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true, gtfs: true },
    { name: stop_timesFieldNamesMap.arrival_time, sqlType: hhmmssSqlType, required: true, gtfs: true },
    { name: stop_timesFieldNamesMap.departure_time, sqlType: hhmmssSqlType, required: true, gtfs: true },
    { name: stop_timesFieldNamesMap.arrival_hms, sqlType: hmsSqlTime, required: true, gtfs: false },
    { name: stop_timesFieldNamesMap.departure_hms, sqlType: hmsSqlTime, required: true, gtfs: false },
    { name: stop_timesFieldNamesMap.stop_id, sqlType: autoIncIdSqlType, gtfs: true },
    { name: stop_timesFieldNamesMap.stop_sequence, sqlType: sequenceSqlType, primaryKey: true, required: true, gtfs: true },
    { name: stop_timesFieldNamesMap.stop_headsign, sqlType: longSqlCharType, required: false, gtfs: true},
    { name: stop_timesFieldNamesMap.pickup_type, sqlType: byteSqlType, required: false, gtfs: true },
    { name: stop_timesFieldNamesMap.drop_off_type, sqlType: byteSqlType, required: false, gtfs: true },
    { name: stop_timesFieldNamesMap.shape_dist_traveled, sqlType: distanceSqlType, required: false, gtfs: true },
    { name: stop_timesFieldNamesMap.timepoint, sqlType: byteSqlType, required: false, gtfs: true }
];
const stop_timesModel = stop_timesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const GTFSModels = [
    { io: 0, name: csvFileNamesMap.agency, required: true, fieldNamesArray: agencyFieldNamesArray, fieldNamesMap: agencyFieldNamesMap, fieldsArray: agencyFieldsArray, model: agencyModel },
    { io: 1, name: csvFileNamesMap.routes, required: true, fieldNamesArray: routesFieldNamesArray, fieldNamesMap: routesFieldNamesMap, fieldsArray: routesFieldsArray, model: routesModel },
    { io: 1, name: csvFileNamesMap.stops, required: true, fieldNamesArray: stopsFieldNamesArray, fieldNamesMap: stopsFieldNamesMap, fieldsArray: stopsFieldsArray, model: stopsModel },
    { io: 1, name: csvFileNamesMap.shapes, required: false, fieldNamesArray: shapesFieldNamesArray, fieldNamesMap: shapesFieldNamesMap, fieldsArray: shapesFieldsArray, model: shapesModel },
    { io: 1, name: csvFileNamesMap.calendar, required: false, fieldNamesArray: calendarFieldNamesArray, fieldNamesMap: calendarFieldNamesMap, fieldsArray: calendarFieldsArray, model: calendarModel },
    { io: 2, name: csvFileNamesMap.calendar_dates, required: false, fieldNamesArray: calendar_datesFieldNamesArray, fieldNamesMap: calendar_datesFieldNamesMap, fieldsArray: calendar_datesFieldsArray, model: calendar_datesModel },
    { io: 3, name: csvFileNamesMap.trips, required: false, fieldNamesArray: tripsFieldNamesArray, fieldNamesMap: tripsFieldNamesMap, fieldsArray: tripsFieldsArray, model: tripsModel },
    { io: 4, name: csvFileNamesMap.stop_times, required: false, fieldNamesArray: stop_timesFieldNamesArray, fieldNamesMap: stop_timesFieldNamesMap, fieldsArray: stop_timesFieldsArray, model: stop_timesModel }
];

const GTFSModelsMap = GTFSModels.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const customGTFSNamesArray = ['shapes_compressed', 'stop_sequences', 'stop_distances', 'stop_hms_offsets', 'trips_sseqs',
    'routes_sseqs', 'routes_shapes', 'stops_sseqs', 'routes_directions', 'routes_shape', 'current_info'];
const customGTFSNamesMap = customGTFSNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});

const shapes_compressedFieldNamesArray = [autoIncKeyColumnName, 'shape_id', 'shape_points', 'shape_dist_traveled', 'shape_original_points', 'shape_original_dist_traveled'];
const shapes_compressedFieldNamesMap = shapes_compressedFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const shapes_compressedFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true },
    { name: shapes_compressedFieldNamesMap.shape_id, sqlType: idSqlType, uniqueKey: true, required: true },
    { name: shapes_compressedFieldNamesMap.shape_points, sqlType: blobSqlType, required: true },
    { name: shapes_compressedFieldNamesMap.shape_dist_traveled, sqlType: blobSqlType, required: true },
    { name: shapes_compressedFieldNamesMap.shape_original_points, sqlType: blobSqlType, required: true },
    { name: shapes_compressedFieldNamesMap.shape_original_dist_traveled, sqlType: blobSqlType, required: true }
];
const shapes_compressedModel = shapes_compressedFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const stop_sequencesFieldNamesArray = [autoIncKeyColumnName, 'route_id', 'route_id_in_agency', 'direction_id', 'gtfs_direction', 'stop_count', 'stop_ids', 'trip_headsign'];
const stop_sequencesFieldNamesMap = stop_sequencesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
let stopSequencesIndex = 1;
const stop_sequencesFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true },
    { name: stop_sequencesFieldNamesMap.route_id, sqlType: autoIncIdSqlType, required: true, gtfs: false },
    { name: stop_sequencesFieldNamesMap.route_id_in_agency, sqlType: idSqlType, required: true, gtfs: false },
    { name: stop_sequencesFieldNamesMap.direction_id, sqlType: byteSqlType, required: true },
    { name: stop_sequencesFieldNamesMap.gtfs_direction, sqlType: byteSqlType, required: true },
    { name: stop_sequencesFieldNamesMap.stop_count, sqlType: countSqlType, required: true },
    { name: stop_sequencesFieldNamesMap.stop_ids, sqlType: blobSqlType, required: true },
    { name: stop_sequencesFieldNamesMap.trip_headsign, sqlType: longSqlCharType, index: stopSequencesIndex++, required: false, gtfs: true },
];
const stop_sequencesModel = stop_sequencesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const stop_distancesFieldNamesArray = [autoIncKeyColumnName, 'stop_count', 'stop_dists'];
const stop_distancesFieldNamesMap = stop_distancesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const stop_distancesFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true },
    { name: stop_distancesFieldNamesMap.stop_count, sqlType: countSqlType, required: true },
    { name: stop_distancesFieldNamesMap.stop_dists, sqlType: blobSqlType, required: true }
];
const stop_distancesModel = stop_distancesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const stop_hms_offsetsFieldNamesArray = [autoIncKeyColumnName,
    'stop_count', 'arrival_hms_offsets', 'departure_hms_offsets', 'pickup_types_encoded', 'drop_off_types_encoded', 'timepoints_encoded', 'stop_headsigns'
];
const stop_hms_offsetsFieldNamesMap = stop_hms_offsetsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const stop_hms_offsetsFieldsArray = [
    { name: autoIncKeyColumnName, sqlType: autoIncIdSqlType, autoIncrement: true, primaryKey: true, required: true },
    { name: stop_hms_offsetsFieldNamesMap.stop_count, sqlType: countSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.arrival_hms_offsets, sqlType: blobSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.departure_hms_offsets, sqlType: blobSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.pickup_types_encoded, sqlType: blobSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.drop_off_types_encoded, sqlType: blobSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.timepoints_encoded, sqlType: blobSqlType, required: true },
    { name: stop_hms_offsetsFieldNamesMap.stop_headsigns, sqlType: blobSqlType, required: true }
];
const stop_hms_offsetsModel = stop_hms_offsetsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const trips_sseqsFieldNamesArray = ['trip_id', 'sseq_id', 'sdist_id', 'stimes_id', 'start_hms', 'end_hms'];
const trips_sseqsFieldNamesMap = trips_sseqsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const trips_sseqsFieldsArray = [
    { name: trips_sseqsFieldNamesMap.trip_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: trips_sseqsFieldNamesMap.sseq_id, sqlType: autoIncIdSqlType, index: 1, required: true },
    { name: trips_sseqsFieldNamesMap.sdist_id, sqlType: autoIncIdSqlType, index: 2, required: true },
    { name: trips_sseqsFieldNamesMap.stimes_id, sqlType: autoIncIdSqlType, index: 3, required: true },
    { name: trips_sseqsFieldNamesMap.start_hms, sqlType: autoIncIdSqlType, index: 4, required: true },
    { name: trips_sseqsFieldNamesMap.end_hms, sqlType: autoIncIdSqlType, index: 5, required: true }
];
const trips_sseqsModel = trips_sseqsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const routes_sseqsFieldNamesArray = ['route_id', 'sseq_id', 'direction_id'];
const routes_sseqsFieldNamesMap = routes_sseqsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const routes_sseqsFieldsArray = [
    { name: routes_sseqsFieldNamesMap.route_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: routes_sseqsFieldNamesMap.sseq_id, sqlType: autoIncIdSqlType, primaryKey: true, index: 1, required: true },
    { name: routes_sseqsFieldNamesMap.direction_id, sqlType: byteSqlType, required: true }
];
const routes_sseqsModel = routes_sseqsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const routes_shapesFieldNamesArray = ['route_id', 'shape_id', 'direction_id'];
const routes_shapesFieldNamesMap = routes_shapesFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const routes_shapesFieldsArray = [
    { name: routes_shapesFieldNamesMap.route_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: routes_shapesFieldNamesMap.shape_id, sqlType: autoIncIdSqlType, primaryKey: true, index: 1, required: true },
    { name: routes_shapesFieldNamesMap.direction_id, sqlType: byteSqlType, required: true }
];
const routes_shapesModel = routes_shapesFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const stops_sseqsFieldNamesArray = ['stop_id', 'sseq_id', 'stop_sequence'];
const stops_sseqsFieldNamesMap = stops_sseqsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const stops_sseqsFieldsArray = [
    { name: stops_sseqsFieldNamesMap.stop_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: stops_sseqsFieldNamesMap.sseq_id, sqlType: autoIncIdSqlType, primaryKey: true, index: 1, required: true },
    { name: stops_sseqsFieldNamesMap.stop_sequence, sqlType: sequenceSqlType, primaryKey: true, index: false, required: true }
];
const stops_sseqsModel = stops_sseqsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const routes_directionsFieldNamesArray = ['route_id', 'direction_id', 'gtfs_direction', 'direction_shape'];
const routes_directionsFieldNamesMap = routes_directionsFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const routes_directionsFieldsArray = [
    { name: routes_directionsFieldNamesMap.route_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: routes_directionsFieldNamesMap.direction_id, sqlType: byteSqlType, primaryKey: true, required: true },
    { name: routes_directionsFieldNamesMap.gtfs_direction, sqlType: byteSqlType, index: 1, required: true },
    { name: routes_directionsFieldNamesMap.direction_shape, sqlType: blobSqlType, required: true }
];
const routes_directionsModel = routes_directionsFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const routes_shapeFieldNamesArray = ['route_id', 'route_shape'];
const routes_shapeFieldNamesMap = routes_shapeFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const routes_shapeFieldsArray = [
    { name: routes_shapeFieldNamesMap.route_id, sqlType: autoIncIdSqlType, primaryKey: true, required: true },
    { name: routes_shapeFieldNamesMap.route_shape, sqlType: blobSqlType, required: true }
];
const routes_shapeModel = routes_shapeFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const current_infoFieldNamesArray = ['agency_id', 'nSubAgencies', 'nServices', 'nRoutes', 'nStops', 'nTrips', 'nStopSeqs', 'nStopDists', 'nStopTimes', 'nShapes', 'extent', 'published_date'];
const current_infoFieldNamesMap = current_infoFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const current_infoFieldsArray = [
    { name: current_infoFieldNamesMap.agency_id, sqlType: idSqlType, primaryKey: true, required: true },
    { name: current_infoFieldNamesMap.nSubAgencies, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nServices, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nRoutes, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nStops, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nTrips, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nStopSeqs, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nStopDists, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nStopTimes, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.nShapes, sqlType: countSqlType, required: true },
    { name: current_infoFieldNamesMap.extent, sqlType: blobSqlType, required: true },
    { name: current_infoFieldNamesMap.published_date, sqlType: dateSqlType, required: true }
];
const current_infoModel = current_infoFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const GTFSCustomModels = [
    { name: customGTFSNamesMap.shapes_compressed, fieldNamesArray: shapes_compressedFieldNamesArray, fieldNamesMap: shapes_compressedFieldNamesMap, fieldsArray: shapes_compressedFieldsArray, model: shapes_compressedModel },
    { name: customGTFSNamesMap.stop_sequences, fieldNamesArray: stop_sequencesFieldNamesArray, fieldNamesMap: stop_sequencesFieldNamesMap, fieldsArray: stop_sequencesFieldsArray, model: stop_sequencesModel },
    { name: customGTFSNamesMap.stop_distances, fieldNamesArray: stop_distancesFieldNamesArray, fieldNamesMap: stop_distancesFieldNamesMap, fieldsArray: stop_distancesFieldsArray, model: stop_distancesModel },
    { name: customGTFSNamesMap.stop_hms_offsets, fieldNamesArray: stop_hms_offsetsFieldNamesArray, fieldNamesMap: stop_hms_offsetsFieldNamesMap, fieldsArray: stop_hms_offsetsFieldsArray, model: stop_hms_offsetsModel },
    { name: customGTFSNamesMap.trips_sseqs, fieldNamesArray: trips_sseqsFieldNamesArray, fieldNamesMap: trips_sseqsFieldNamesMap, fieldsArray: trips_sseqsFieldsArray, model: trips_sseqsModel },
    { name: customGTFSNamesMap.routes_sseqs, fieldNamesArray: routes_sseqsFieldNamesArray, fieldNamesMap: routes_sseqsFieldNamesMap, fieldsArray: routes_sseqsFieldsArray, model: routes_sseqsModel },
    { name: customGTFSNamesMap.routes_shapes, fieldNamesArray: routes_shapesFieldNamesArray, fieldNamesMap: routes_shapesFieldNamesMap, fieldsArray: routes_shapesFieldsArray, model: routes_shapesModel },
    { name: customGTFSNamesMap.stops_sseqs, fieldNamesArray: stops_sseqsFieldNamesArray, fieldNamesMap: stops_sseqsFieldNamesMap, fieldsArray: stops_sseqsFieldsArray, model: stops_sseqsModel },
    { name: customGTFSNamesMap.routes_directions, fieldNamesArray: routes_directionsFieldNamesArray, fieldNamesMap: routes_directionsFieldNamesMap, fieldsArray: routes_directionsFieldsArray, model: routes_directionsModel },
    { name: customGTFSNamesMap.routes_shape, fieldNamesArray: routes_shapeFieldNamesArray, fieldNamesMap: routes_shapeFieldNamesMap, fieldsArray: routes_shapeFieldsArray, model: routes_shapeModel },
    { name: customGTFSNamesMap.current_info, fieldNamesArray: current_infoFieldNamesArray, fieldNamesMap: current_infoFieldNamesMap, fieldsArray: current_infoFieldsArray, model: current_infoModel }
];
const GTFSCustomModelsMap = GTFSCustomModels.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const GTFSCustomModelsInfo = {
    namesArray: customGTFSNamesArray,
    namesMap: customGTFSNamesMap,
    models: GTFSCustomModels,
    modelsMap: GTFSCustomModelsMap
};

const gtfsModelsToCompare = [
    GTFSModelsMap.routes,
    GTFSModelsMap.calendar,
    GTFSModelsMap.calendar_dates,
    GTFSCustomModelsMap.shapes_compressed,
    GTFSCustomModelsMap.stop_sequences,
    GTFSCustomModelsMap.stop_distances,
    GTFSCustomModelsMap.stop_hms_offsets,
    GTFSCustomModelsMap.stops_sseqs,
    GTFSCustomModelsMap.trips_sseqs,
    GTFSCustomModelsMap.routes_sseqs,
    GTFSCustomModelsMap.routes_directions,
    GTFSCustomModelsMap.routes_shapes
];

const gtfsModelsToCopy = [
    GTFSModelsMap.agency,
    GTFSModelsMap.routes,
    GTFSModelsMap.stops,
    GTFSModelsMap.calendar,
    GTFSModelsMap.calendar_dates,
    GTFSModelsMap.trips,
    GTFSCustomModelsMap.shapes_compressed,
    GTFSCustomModelsMap.stop_sequences,
    GTFSCustomModelsMap.stop_distances,
    GTFSCustomModelsMap.stop_hms_offsets,
    GTFSCustomModelsMap.stops_sseqs,
    GTFSCustomModelsMap.trips_sseqs,
    GTFSCustomModelsMap.routes_sseqs,
    GTFSCustomModelsMap.routes_directions,
    GTFSCustomModelsMap.routes_shape,
    GTFSCustomModelsMap.routes_shapes,
    GTFSCustomModelsMap.current_info
];

//const gtfsModelsToRename = GTFSModels.concat(GTFSCustomModels);
const gtfsModelsToRename = gtfsModelsToCopy.slice(0);

const agency_published_table_name = 'agency_published';
const agency_publishedFieldNamesArray = ['published_date'];
const agency_publishedFieldNamesMap = agency_publishedFieldNamesArray.reduce((prev, cur) => { prev[cur] = cur; return prev; }, {});
const agency_publishedFieldsArray = [
    { name: agency_publishedFieldNamesMap.published_date, sqlType: dateSqlType, primaryKey: true, required: true }
];
const agency_publishedModel = agency_publishedFieldsArray.reduce((prev, cur) => { prev[cur.name] = cur; return prev; }, {});

const getBakTableName = (gtfsAgencyId, gtfsModel) => { return gtfsAgencyId + '_bak_' + gtfsModel.name; }
const getTempTableName = (gtfsAgencyId, gtfsModel) => { return gtfsAgencyId + '_temp_' + gtfsModel.name; }
const getCurrentTableName = (gtfsAgencyId, gtfsModel) => { return gtfsAgencyId + '_current_' + gtfsModel.name; }
const getPublishedTableName = (gtfsAgencyId, gtfsModel, publishedDate) => {
    let dateStr = db.YYYYMMDDToString(publishedDate);
    return gtfsAgencyId + '_published_' + dateStr + '_' + gtfsModel.name;
};
const getAgencyPublishedTableName = gtfsAgencyId => { return gtfsAgencyId + '_' + agency_published_table_name };

const dropTablesInModels = async (models, gtfsAgencyId, getTableName, getTableNameOnDate) => {
    let sql = 'DROP TABLE IF EXISTS ';
    let nModels = models.length; for (let i = 0; i < nModels; ++i) { let gtfsModel = models[i]; let tableName = getTableName(gtfsAgencyId, gtfsModel, getTableNameOnDate); sql += tableName; if (i < nModels - 1) { sql += ','; } }
    sql += ';'
    return db.queryPoolVoid(sql).then(() => { return true; }).catch(err => { console.log('dropBakTables: ' + err.message); return false; });
};

const dropTables = async (gtfsAgencyId, getTableName, getTableNameOnDate) => {
    return dropTablesInModels(GTFSModels, gtfsAgencyId, getTableName, getTableNameOnDate).then(() => {
        return dropTablesInModels(GTFSCustomModels, gtfsAgencyId, getTableName, getTableNameOnDate);
    }).catch(() => {
        return dropTablesInModels(GTFSCustomModels, gtfsAgencyId, getTableName, getTableNameOnDate);
    });
};

const dropBakTables = async (gtfsAgencyId) => { return dropTables(gtfsAgencyId, getBakTableName); };
const dropTempTables = async (gtfsAgencyId) => { return dropTables(gtfsAgencyId, getTempTableName); };
const dropCurrentTables = async (gtfsAgencyId) => { return dropTables(gtfsAgencyId, getCurrentTableName); };
const dropTempAndBakTables = async (gtfsAgencyId) => { return dropBakTables(gtfsAgencyId).then(() => { return dropTempTables(gtfsAgencyId); }).catch(() => { return dropTempTables(gtfsAgencyId); }); };
const dropTempAndBakAndCurrentTables = async (gtfsAgencyId) => { dropTempAndBakTables(gtfsAgencyId).then(() => { return dropCurrentTables(gtfsAgencyId); }).catch(() => { return dropCurrentTables(gtfsAgencyId); }); };

const dropPublishedTables = async (gtfsAgencyId, onDate) => { return dropTables(gtfsAgencyId, getPublishedTableName, onDate); };

const dropAllAgencyTables = async (gtfsAgencyId) => { return dropTempAndBakAndCurrentTables(gtfsAgencyId).then(() => { return true }); };

const checkAgencyHasCurrentSet = async gtfsAgencyId => { return db.checkIfTableExists(getCurrentTableName(gtfsAgencyId, GTFSModelsMap.stops)); };
const checkHasPublishedOnDate = async (gtfsAgencyId, onDate) => { return db.checkIfTableExists(getPublishedTableName(gtfsAgencyId, GTFSModelsMap.stops, onDate)); };

const compareModelTable = async (gtfsAgencyId, model, getTableName1, getTableName1OnDate, getTableName2, getTableName2OnDate) => {
    let tableName1 = getTableName1(gtfsAgencyId, model, getTableName1OnDate);
    let tableName2 = getTableName2(gtfsAgencyId, model, getTableName2OnDate);
    let sqlStr = "SELECT COUNT(*)FROM(SELECT ?? ";
    let values = [], fieldNamesArray = model.fieldNamesArray;
    values.push(fieldNamesArray);
    sqlStr += " FROM ?? UNION ALL SELECT ?? ";
    values.push(tableName1, fieldNamesArray);
    sqlStr += " FROM ??) ut GROUP BY ?? ";
    values.push(tableName2, fieldNamesArray);
    sqlStr += " HAVING COUNT(*) = 1";
    return db.queryPoolValues(sqlStr, values).then(results => { return results.length > 0; }).catch(err => {
        if (err.code != 'ER_NO_SUCH_TABLE') { console.log('compareModelTable(' + tableName1 + ', ' + tableName2 + ') ' + err.message); }
        return true;
    });
};

const compareAgencyVersions = async (gtfsAgencyId, getTableName1, getTableName1OnDate, getTableName2, getTableName2OnDate) => {
    let comparePromises = gtfsModelsToCompare.map(t => { return Promise.resolve(compareModelTable(gtfsAgencyId, t, getTableName1, getTableName1OnDate, getTableName2, getTableName2OnDate)).reflect(); });
    let agenciesDiffer = false;
    return Promise.all(
        comparePromises
    ).each(inspection => {
        if (inspection.isFulfilled()) {
            if (inspection.value()) {
                agenciesDiffer = true;
            }
        }
        else { throw (inspection.reason()); }
    }).then(result => {
        return true;
        return agenciesDiffer;
    }).catch(err => {
        console.log('compareAgencyVersions: ' + err.message);
        return true;
    });
};

const compareTempAndCurrentAgency = async (gtfsAgencyId) => { return compareAgencyVersions(gtfsAgencyId, getTempTableName, undefined, getCurrentTableName, undefined); };
const compareCurrentAndPublishedAgency = async (gtfsAgencyId) => {
    let lastPublishedDate = getAgency().getAgencyPublications().GetLatestPublishDateByAgencyId(gtfsAgencyId);
    return compareAgencyVersions(gtfsAgencyId, getCurrentTableName, undefined, getPublishedTableName, lastPublishedDate);
};

const copyModelTable = async (gtfsAgencyId, model, getTableNameDest, getTableNameDestOnDate, getTableNameSource, getTableNameSourceOnDate) => {
    let tableNameDest = getTableNameDest(gtfsAgencyId, model, getTableNameDestOnDate);
    let tableNameSource = getTableNameSource(gtfsAgencyId, model, getTableNameSourceOnDate);
    let sqlStr = "CREATE TABLE ?? LIKE ??";
    let values = [tableNameDest, tableNameSource];
    return db.queryPoolValues(sqlStr, values).then(results => {
        sqlStr = "INSERT ?? SELECT * FROM ??;";
        return db.queryPoolValues(sqlStr, values);
    }).catch(err => {
        if (err.code != 'ER_NO_SUCH_TABLE') { console.log('copyModelTable(' + tableNameDest + ', ' + tableNameSource + ') ' + err.message); }
        return true;
    });
};

const copyAgencyVersions = async (gtfsAgencyId, getTableNameDest, getTableNameDestOnDate, getTableNameSource, getTableNameSourceOnDate) => {
    let copyPromises = gtfsModelsToCopy.map(t => { return Promise.resolve(copyModelTable(gtfsAgencyId, t, getTableNameDest, getTableNameDestOnDate, getTableNameSource, getTableNameSourceOnDate)).reflect(); });
    return Promise.all(
        copyPromises
    ).each(inspection => {
        if (inspection.isFulfilled()) { if (inspection.value()) { } }
        else { throw (inspection.reason()); }
    }).then(result => {
        return true;
    }).catch(err => {
        console.log('compareAgencyVersions: ' + err.message);
        return false;
    });
};

const copyCurrentToTemp = async (gtfsAgencyId) => { return copyAgencyVersions(gtfsAgencyId, getTempTableName, undefined, getCurrentTableName, undefined); };
const copyCurrentToPublished = async (gtfsAgencyId, onDate) => { return copyAgencyVersions(gtfsAgencyId, getPublishedTableName, onDate, getCurrentTableName, undefined); };

const makeRenameSql = (gtfsAgencyId, models, getOldName, getOldDate, getNewName, getNewDate, getBakName, getBakDate) => {
    let sql = 'RENAME TABLE ';
    let nModels = models.length;
    for (let i = 0; i < nModels; ++i) {
        let gtfsModel = models[i];
        let oldName = getOldName(gtfsAgencyId, gtfsModel, getOldDate);
        let newName = getNewName(gtfsAgencyId, gtfsModel, getNewDate);
        if (getBakName) {
            let bakName = getBakName(gtfsAgencyId, gtfsModel, getBakDate);
            sql += newName + ' TO ' + bakName + ',';
        }
        sql += oldName + ' TO ' + newName;
        if (i < nModels - 1) { sql += ','; }
    }
    sql += ';'
    //console.log('makeRenameSql: ' + sql);
    return sql;
};

const renameAgencyVersions = async (gtfsAgencyId, getOldName, getOldDate, getNewName, getNewDate, getBakName, getBakDate) => {
    let renameSql = makeRenameSql(gtfsAgencyId, gtfsModelsToRename, getOldName, getOldDate, getNewName, getNewDate, getBakName, getBakDate);
    //console.log(renameSql);
    return db.queryPoolVoid(renameSql).then(() => { return true; }).catch(err => { console.log('renameAgencyVersions: ' + err.message); return false; });
};

const renameTempToPublished = async (gtfsAgencyId, onDate) => { return renameAgencyVersions(gtfsAgencyId, getTempTableName, undefined, getPublishedTableName, onDate, getBakTableName, undefined); };
const renameTempToCurrent = async (gtfsAgencyId, oldSetExists) => { return renameAgencyVersions(gtfsAgencyId, getTempTableName, undefined, getCurrentTableName, undefined, oldSetExists ? getBakTableName : undefined, undefined); };

let globalImportId = 0;

const gtfsDefaultStopDistanceRadiusInMeters = 200;

const orderByLead = ' ORDER BY ';
const orderByTrail = 'CONVERT(??, UNSIGNED INT) ASC, ?? ASC ';
const orderByStrId = orderByLead + orderByTrail;
const orderByTwoKeysStrId = orderByLead + orderByTrail + ', ' + orderByTrail;

const orderByTableQualifiedTrail = 'CONVERT(??.??, UNSIGNED INT) ASC, ??.?? ASC ';
const orderByTableQualifiedStrId = orderByLead + orderByTableQualifiedTrail;

const getDecodeData = (polyCode, data, isCoordinates, precision, returnGeoJSON) => {
    let polyCodeUse = polyCode ? polyCode : geom.PolyCode();
    let decoded;
    if (isCoordinates) {
        let type;
        if (typeof data === 'string') {
            type = 'linestring';
            decoded = polyCode.DecodeLineString(data, precision);
        }
        else {
            let dataLen = data.length;
            type = 'multilinestring';
            decoded = new Array(dataLen);
            for (let i = 0; i < dataLen; ++i) { decoded[i] = polyCode.DecodeLineString(data[i], precision); }
        }
        if (returnGeoJSON) { decoded = { type: type, coordinates: decoded }; }
    }
    else { decoded = polyCode.DecodeValues(data, precision); }
    return decoded;
};


const decodeStopSequences = (stopSequences, polyCode) => {
    if (stopSequences) {
        let nSequences = stopSequences.length;
        if (nSequences > 0) {
            let polyCodeUse = polyCode ? polyCode : geom.PolyCode();
            let m = stop_sequencesFieldNamesMap;
            for (let i = 0; i < nSequences; ++i) {
                let r = stopSequences[i];
                r[m.stop_ids] = getDecodeData(polyCodeUse, r[m.stop_ids], false, 0, false);
            }
        }
    }
    return stopSequences;
};

const decodeStopTimes = (stopTimes, decodeData, polyCode) => {
    if (stopTimes) {
        let nSequences = stopTimes.length;
        if (nSequences > 0) {
            let polyCodeUse = polyCode ? polyCode : geom.PolyCode();
            let m = stop_hms_offsetsFieldNamesMap;
            for (let i = 0; i < nSequences; ++i) {
                let r = stopTimes[i];
                if (decodeData) {
                    r[m.arrival_hms_offsets] = getDecodeData(polyCodeUse, r[m.arrival_hms_offsets], false, 0, false);
                    r[m.departure_hms_offsets] = getDecodeData(polyCodeUse, r[m.departure_hms_offsets], false, 0, false);
                    r[m.pickup_types_encoded] = getDecodeData(polyCodeUse, r[m.pickup_types_encoded], false, 0, false);
                    r[m.drop_off_types_encoded] = getDecodeData(polyCodeUse, r[m.drop_off_types_encoded], false, 0, false);
                    r[m.timepoints_encoded] = getDecodeData(polyCodeUse, r[m.timepoints_encoded], false, 0, false);
                }
                r[m.stop_headsigns] = JSON.parse(r[m.stop_headsigns]);
            }
        }
    }
    return stopTimes;
};

const decodeStopDistances = (stopDistances, polyCode) => {
    if (stopDistances) {
        let nDistances = stopDistances.length;
        if (nDistances > 0) {
            let polyCodeUse = polyCode ? polyCode : geom.PolyCode();
            let m = stop_distancesFieldNamesMap;
            for (let i = 0; i < nDistances; ++i) {
                let r = stopDistances[i];
                r[m.stop_dists] = getDecodeData(polyCodeUse, r[m.stop_dists], false, distancesPrecision, false);
            }
        }
    }
    return stopDistances;
};

const decodeRoutesShape = (routesShape, polyCode, returnGeoJSON) => {
    if (routesShape) {
        let count = routesShape.length;
        if (count > 0) {
            let polyCodeUse = polyCode ? polyCode : geom.PolyCode();
            let m = routes_shapeFieldNamesMap;
            for (let i = 0; i < count; ++i) {
                let r = routesShape[i];
                r[m.route_shape] = getDecodeData(polyCodeUse, JSON.parse(r[m.route_shape]), true, lineStringPrecision, returnGeoJSON);
            }
        }
    }
    return routesShape;
};

const getAgencyPublishedDates = async (gtfsAgencyId) => {
    let tableName = getAgencyPublishedTableName(gtfsAgencyId);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ?? ORDER BY ?? ASC;";
        let values = [agency_publishedFieldNamesArray, tableName, agency_publishedFieldNamesMap.published_date];
        return db.queryPoolValues(sqlStr, values).then(results => {
            return results.map(t => { return { agency_id: gtfsAgencyId, published_date: t[agency_publishedFieldNamesMap.published_date] }; });
        }).catch(err => {
            if (err.code !== "ER_NO_SUCH_TABLE") { console.log('getAgencyPublishedDates: ' + err); }
            return [];
        });
    }
    else { return []; }
};

const getAgenciesPublishedDates = async (agencyIds) => {
    if (agencyIds && agencyIds.length > 0) {
        let fillAgencyPromises = agencyIds.map(t => { return Promise.resolve(getAgencyPublishedDates(t)).reflect(); });
        let agencyPublishedDates = [];
        return Promise.all(
            fillAgencyPromises
        ).each(inspection => {
            if (inspection.isFulfilled()) {
                let value = inspection.value();
                if (value) { agencyPublishedDates.push.apply(agencyPublishedDates, value); }
            }
            else {
                console.log('getAgenciesPublishedDates: ' + inspection.reason().message);
            }
        }).then(result => {
            return agencyPublishedDates;
        });
    }
    else {
        return [];
    }
};

const getLatestAgencyPublishedDates = async (gtfsAgencyId) => {
    let tableName = getAgencyPublishedTableName(gtfsAgencyId);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ?? ORDER BY ?? DESC LIMIT 1;";
        let values = [agency_publishedFieldNamesArray, tableName, agency_publishedFieldNamesMap.published_date];
        return db.queryPoolValues(sqlStr, values).then(results => {
            return results.map(t => { return { agency_id: gtfsAgencyId, published_date: t[agency_publishedFieldNamesMap.published_date] }; });
        }).catch(err => {
            if (err.code !== "ER_NO_SUCH_TABLE") { console.log('getLatestAgencyPublishedDates: ' + err); }
            return [];
        });
    }
    else { return []; }
};

const getLatestAgenciesPublishedDates = async (agencyIds) => {
    if (agencyIds && agencyIds.length > 0) {
        let fillAgencyPromises = agencyIds.map(t => { return Promise.resolve(getLatestAgencyPublishedDates(t)).reflect(); });
        let agencyPublishedDates = [];
        return Promise.all(
            fillAgencyPromises
        ).each(inspection => {
            if (inspection.isFulfilled()) {
                let value = inspection.value();
                if (value) { agencyPublishedDates.push.apply(agencyPublishedDates, value); }
            }
            else {
                console.log('getLatestAgenciesPublishedDates: ' + inspection.reason().message);
            }
        }).then(result => {
            return agencyPublishedDates;
        });
    }
    else {
        return [];
    }
};

const getAgencyInfo = async (gtfsAgencyId, getTableName, getTableDate) => {
    let gtfsModel = GTFSCustomModelsMap.current_info;
    let tableName = getTableName(gtfsAgencyId, gtfsModel, getTableDate);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ??";
        let values = [current_infoFieldNamesArray, tableName];
        return db.queryPoolValues(sqlStr, values).then(results => {
            let result = results.length > 0 ? results[0] : undefined;
            if (result) { result[current_infoFieldNamesMap.extent] = JSON.parse(result[current_infoFieldNamesMap.extent]); }
            return result;
        }).catch(err => {
            if (err.code !== "ER_NO_SUCH_TABLE") { console.log('getCurrentAgencyInfo: ' + err); }
            return undefined;
        });
    }
    else { return undefined; }
};

const getAgencyInfos = async (agencyIds, getInfo, getInfoOnDates) => {
    let fillAgencyPromises = agencyIds.map((t, index) => { let getInfoOnDate = getInfoOnDates ? getInfoOnDates[index] : undefined; return Promise.resolve(getInfo(t, getInfoOnDate)).reflect(); });
    let agencyInfos = [];
    return Promise.all(
        fillAgencyPromises
    ).each(inspection => {
        if (inspection.isFulfilled()) {
            let value = inspection.value();
            if (value) { agencyInfos.push(value); }
        }
        else {
            console.log('getAgencyInfos: ' + inspection.reason().message);
        }
    }).then(result => {
        return agencyInfos;
    });
};

const getCurrentAgencyInfo = async (gtfsAgencyId) => { return getAgencyInfo(gtfsAgencyId, getCurrentTableName, undefined); };
const getCurrentAgencyInfos = async (agencyIds) => { return getAgencyInfos(agencyIds, getCurrentAgencyInfo); };

const getPublishedAgencyInfo = async (gtfsAgencyId, onDate) => { return getAgencyInfo(gtfsAgencyId, getPublishedTableName, onDate); };
const getPublishedAgencyInfos = async (gtfsAgencyIds, onDates) => { return getAgencyInfos(gtfsAgencyIds, getPublishedAgencyInfo, onDates); };

const getLatestPublishedAgencyInfos = async (agencyIds) => {
    return getLatestAgenciesPublishedDates(
        agencyIds
    ).then(results => {
        let nResults = results.length;
        if (nResults > 0) {
            let idsAndInds = getAgency().getAgencyIdsAndIndsFromResults(results, "agency_id");
            let onDates = [];
            let nAgencyIds = agencyIds.length;
            let agencyIdsToGetPublishedInfo = [];
            for (let i = 0; i < nAgencyIds; ++i) {
                let agencyId = agencyIds[i];
                let ind = idsAndInds.agencyInds['' + agencyId];
                if (ind !== undefined) {
                    agencyIdsToGetPublishedInfo.push(agencyId);
                    onDates.push(results[ind].published_date);
                }
            }
            if (onDates.length > 0) {
                return getPublishedAgencyInfos(agencyIdsToGetPublishedInfo, onDates);
            }
            else {
                return [];
            }
        }
        else {
            return [];
        }
    });
};

//const getCurrentRouteByRouteAgencyId = async (gtfsAgencyId, routeAgencyId) => { return getCurrentRoutes(gtfsAgencyId, undefined, undefined, undefined, routeAgencyId).then(results => { return db.getFirstResult(results); }); };
//const getCurrentServicesByServiceAgencyId = async (gtfsAgencyId, serviceAgencyIds) => { return getCurrentServices(gtfsAgencyId, undefined, serviceAgencyIds); };
//const getCurrentTripByTripAgencyId = async (gtfsAgencyId, tripAgencyId) => { return getCurrentTrips(gtfsAgencyId, undefined, tripAgencyId).then(results => { return db.getFirstResult(results); }); };
//const getCurrentStopByStopAgencyId = async (gtfsAgencyId, stopAgencyId) => { return getCurrentStops(gtfsAgencyId, undefined, stopAgencyId).then(results => { return db.getFirstResult(results); }); };

const getStopsByStopAgencyId = async (gtfsAgencyId, getTableName, getTableNameDate, stopAgencyIds) => { return getStops(gtfsAgencyId, getTableName, getTableNameDate, undefined, stopAgencyIds); };
const getRouteByRouteAgencyId = async (gtfsAgencyId, getTableName, getTableNameDate, routeAgencyId) => { return getRoutes(gtfsAgencyId, getTableName, getTableNameDate, undefined, undefined, undefined, routeAgencyId).then(results => { return db.getFirstResult(results); }); };
const getStopByStopAgencyId = async (gtfsAgencyId, getTableName, getTableNameDate, stopAgencyId) => { return getStops(gtfsAgencyId, getTableName, getTableNameDate, undefined, stopAgencyId).then(results => { return db.getFirstResult(results); }); };
const getServicesByServiceAgencyId = async (gtfsAgencyId, getTableName, getTableNameDate, serviceAgencyIds) => { return getServices(gtfsAgencyId, getTableName, getTableNameDate, undefined, serviceAgencyIds); };

const getAgencies = async (gtfsAgencyId, getTableName, getTableNameDate, agencyId, agencyIdInAgency) => {
    let gtfsModel = GTFSModelsMap.agency;
    let tableName = getTableName(gtfsAgencyId, gtfsModel, getTableNameDate);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ??";
        let values = [agencyFieldNamesArray, tableName];
        if (agencyId || agencyIdInAgency) {
            let agencyIdUse = agencyId ? agencyId : agencyIdInAgency;
            if (agencyIdUse.length !== undefined) { sqlStr += " WHERE ?? IN (?)"; } else { sqlStr += " WHERE ?? = ?"; }
            if (agencyId) { values.push.apply(values, [autoIncKeyColumnName, agencyId]); }
            else { values.push.apply(values, [agencyFieldNamesMap.agency_id, agencyIdInAgency]); }
        }
        sqlStr += orderByStrId;
        values.push.apply(values, [agencyFieldNamesMap.agency_id, agencyFieldNamesMap.agency_id]);
        return db.queryPoolValues(sqlStr, values).then(results => { return results; }).catch(err => { console.log('getCurrentAgencies: ' + err); return []; });
    }
    else { return []; }
};


const getCurrentAgencies = async (gtfsAgencyId, agencyId, agencyIdInAgency) => { return getAgencies(gtfsAgencyId, getCurrentTableName, undefined, agencyId, agencyIdInAgency); };

const getPublishedAgencies = async (gtfsAgencyId, onTransitDate, agencyId, agencyIdInAgency) => { return getAgencies(gtfsAgencyId, getPublishedTableName, onTransitDate, agencyId, agencyIdInAgency); };

const getRoutesDirections = async (gtfsAgencyId, getTableName, getTableNameDate, routeIds, includeShape, decodeData, returnGeoJSON) => {
    let routesDirectionsTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.routes_directions, getTableNameDate);
    let sqlStr = "select ?? from ?? where ?? in (?);"
    if (routeIds.length === undefined) { routeIds = [routeIds]; }
    let values = [routes_directionsFieldNamesArray, routesDirectionsTableName, routes_directionsFieldNamesMap.route_id, routeIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        let nResults = results.length;
        let polyCode = geom.PolyCode();
        for (let i = 0; i < nResults; ++i) {
            let r = results[i];
            if (!includeShape) {
                delete r[routes_directionsFieldNamesMap.direction_shape];
            }
            else if (decodeData || returnGeoJSON) {
                r[routes_directionsFieldNamesMap.direction_shape] = getDecodeData(polyCode,
                    JSON.parse(r[routes_directionsFieldNamesMap.direction_shape]), true, lineStringPrecision, returnGeoJSON);
            }
            let gtfs_direction = r[routes_directionsFieldNamesMap.gtfs_direction];
            r[gtfs_direction_name_FieldName] = gtfsDirectionFriendlyNamesArray[gtfs_direction];
        }
        return results;
    }).catch(err => { console.log('getRoutesDirections: ' + err); throw (err); });
};

const getRoutesShape = async (gtfsAgencyId, getTableName, getTableNameDate, routeIds, decodeData, returnGeoJSON) => {
    let routesShapeTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.routes_shape, getTableNameDate);
    let sqlStr = "select ?? from ?? where ?? in (?);"
    if (routeIds.length === undefined) { routeIds = [routeIds]; }
    let values = [routes_shapeFieldNamesArray, routesShapeTableName, routes_shapeFieldNamesMap.route_id, routeIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        let nResults = results.length;
        if (nResults > 0) { if (decodeData || returnGeoJSON) { results = decodeRoutesShape(results, undefined, returnGeoJSON); } }
        return results;
    }).catch(err => { console.log('getRoutesShape: ' + err); throw (err); });
};

const getRoutesServiceIds = async (gtfsAgencyId, getTableName, getTableNameDate, routeIds) => {
    //select ct.route_id, ct.service_id from 5_current_trips ct group by ct.route_id, ct.service_id order by ct.route_id, ct.service_id;
    let tripsTableName = getTableName(gtfsAgencyId, GTFSModelsMap.trips, getTableNameDate);
    let sqlStr = "select ??, ?? from ?? where ?? in (?) group by ??, ?? order by ?? asc, ?? asc;"
    if (routeIds.length === undefined) { routeIds = [routeIds]; }
    let values = [tripsFieldNamesMap.route_id, tripsFieldNamesMap.service_id, tripsTableName, tripsFieldNamesMap.route_id, routeIds, tripsFieldNamesMap.route_id, tripsFieldNamesMap.service_id, tripsFieldNamesMap.route_id, tripsFieldNamesMap.service_id];
    return db.queryPoolValues(sqlStr, values).then(results => {
        //let nResults = results.length;
        //if (nResults > 0) { if (decodeData || returnGeoJSON) { results = decodeRoutesShape(results, undefined, returnGeoJSON); } }
        return results;
    }).catch(err => { console.log('getRoutesServiceIds: ' + err); throw (err); });
};

const getRoutesStopSeqsIds = async (gtfsAgencyId, getTableName, getTableNameDate, routeIds) => {
    let routeSSeqsTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.routes_sseqs, getTableNameDate);
    let sqlStr = "select ??, ?? from ?? where ?? in (?) order by ?? asc, ?? asc;"
    if (routeIds.length === undefined) { routeIds = [routeIds]; }
    let values = [routes_sseqsFieldNamesMap.route_id, routes_sseqsFieldNamesMap.sseq_id, routeSSeqsTableName, routes_sseqsFieldNamesMap.route_id, routeIds, routes_sseqsFieldNamesMap.route_id, routes_sseqsFieldNamesMap.sseq_id];
    return db.queryPoolValues(sqlStr, values).then(results => {
        return results;
    }).catch(err => { console.log('getRoutesStopSeqsIds: ' + err); throw (err); });
};

const getRoutes = async (
    gtfsAgencyId, getTableName, getTableNameDate, agencyId, agencyIdInAgency, routeId, routeIdInAgency,
    includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON) => {
    let tableName = getTableName(gtfsAgencyId, GTFSModelsMap.routes, getTableNameDate);
    let hasIncludes = includeDirections || includeDirectionShape || includeRouteShape || includeServiceIds || includeStopSequenceIds;
    if (tableName) {
        return new Promise((resolve, reject) => {
            let agencyIdNeeded = (agencyId || agencyIdInAgency);
            let agencyIdKnown = !!agencyId;
            let currentRouteResultsIndexByRouteId, currentRouteResults;

            const onAllIncluded = () => { resolve(currentRouteResults); };

            const onIncludeRouteStopSequenceIds = () => {
                if (includeStopSequenceIds) {
                    return getRoutesStopSeqsIds(
                        gtfsAgencyId, getTableName, getTableNameDate, routeId
                    ).then(results => {
                        for (let i in results) {
                            let stopSequenceResult = results[i];
                            let resultRouteId = stopSequenceResult[routes_sseqsFieldNamesMap.route_id];
                            let routeResult = currentRouteResults[currentRouteResultsIndexByRouteId['' + resultRouteId]];
                            if (routeResult !== undefined) {
                                let theStopSequenceId = stopSequenceResult[routes_sseqsFieldNamesMap.sseq_id];
                                routeResult.stopSequenceIds.push(routeResult.stopSequenceIdsMap[theStopSequenceId] = theStopSequenceId);
                            }
                        }
                        onAllIncluded();
                    });
                }
                else { onAllIncluded(); }
            };

            const onIncludeRouteServiceIds = () => {
                if (includeServiceIds) {
                    return getRoutesServiceIds(
                        gtfsAgencyId, getTableName, getTableNameDate, routeId
                    ).then(results => {
                        for (let i in results) {
                            let serviceResult = results[i];
                            let resultRouteId = serviceResult[tripsFieldNamesMap.route_id];
                            let routeResult = currentRouteResults[currentRouteResultsIndexByRouteId['' + resultRouteId]];
                            if (routeResult !== undefined) {
                                let theServiceId = serviceResult[tripsFieldNamesMap.service_id];
                                routeResult.serviceIds.push(routeResult.serviceIdsMap[theServiceId] = theServiceId);
                            }
                        }
                        onIncludeRouteStopSequenceIds();
                    });
                }
                else { onIncludeRouteStopSequenceIds(); }
            };

            const onIncludeRouteShape = () => {
                if (includeRouteShape) {
                    return getRoutesShape(
                        gtfsAgencyId, getTableName, getTableNameDate, routeId, decodeData, returnGeoJSON
                    ).then(results => {
                        for (let i in results) {
                            let shapeResult = results[i];
                            let resultRouteId = shapeResult[routes_shapesFieldNamesMap.route_id];
                            let routeResult = currentRouteResults[currentRouteResultsIndexByRouteId['' + resultRouteId]];
                            if (routeResult !== undefined) {
                                delete shapeResult[routes_shapesFieldNamesMap.route_id];
                                routeResult.route_shape = shapeResult;
                            }
                        }
                        onIncludeRouteServiceIds();
                    });
                }
                else { onIncludeRouteServiceIds(); }
            };

            const onIncludeDirections = () => {
                if (includeDirections) {
                    return getRoutesDirections(
                        gtfsAgencyId, getTableName, getTableNameDate, routeId, includeDirectionShape, decodeData, returnGeoJSON
                    ).then(results => {
                        for (let i in results) {
                            let dirResult = results[i];
                            let dirResultRouteId = dirResult[routes_directionsFieldNamesMap.route_id];
                            let routeResult = currentRouteResults[currentRouteResultsIndexByRouteId['' + dirResultRouteId]];
                            if (routeResult !== undefined) {
                                delete dirResult[routes_directionsFieldNamesMap.route_id];
                                routeResult.directions.push(dirResult);
                            }
                        }
                        onIncludeRouteShape();
                    });
                }
                else { onIncludeRouteShape(); }
            };

            const onAgencyIdKnown = () => {
                let sqlStr = "SELECT ?? FROM ??";
                let andOrWhereStr = " WHERE ";
                let values = [routesFieldNamesArray, tableName];
                if (agencyId) {
                    sqlStr += " WHERE ?? = ?";
                    andOrWhereStr = " AND "
                    values.push.apply(values, [routesFieldNamesMap.agency_id, agency[autoIncKeyColumnName]]);
                }
                if (routeId || routeIdInAgency) {
                    let routeIdUse = routeId ? routeId : routeIdInAgency;
                    if (routeIdUse.length !== undefined) { sqlStr += andOrWhereStr + "?? IN(?)"; } else { sqlStr += andOrWhereStr + "?? = ?"; }
                    if (routeId) { values.push.apply(values, [autoIncKeyColumnName, routeId]); }
                    else { values.push.apply(values, [routesFieldNamesMap.route_id, routeIdInAgency]); }
                }
                sqlStr += orderByStrId;
                values.push.apply(values, [routesFieldNamesMap.route_id, routesFieldNamesMap.route_id]);
                db.queryPoolValues(sqlStr, values).then(results => {
                    let nResults = results.length;
                    currentRouteResults = results;
                    if (nResults && hasIncludes) {
                        let mapAndIds = currentRouteResults.reduce((mapAndIds, cur, index) => {
                            if (includeDirections) { cur.directions = []; }
                            if (includeServiceIds) { cur.serviceIds = []; cur.serviceIdsMap = {}; }
                            if (includeStopSequenceIds) { cur.stopSequenceIds = []; cur.stopSequenceIdsMap = {}; }
                            let curId = cur[autoIncKeyColumnName];
                            mapAndIds.map['' + curId] = index;
                            mapAndIds.ids.push(curId);
                            return mapAndIds;
                        }, { map: {}, ids: [] });
                        currentRouteResultsIndexByRouteId = mapAndIds.map;
                        routeId = mapAndIds.ids;
                        onIncludeDirections();
                    }
                    else { onAllIncluded(); }
                }).catch(err => {
                    console.log('getRoutes: ' + err);
                    resolve([]);
                });
            };

            const resolveAgencyId = () => {
                getAgencies(gtfsAgencyId, getTableName, getTableNameDate, undefined, agencyIdInAgency).then(results => {
                    if (results) { agencyId = results[autoIncKeyColumnName]; onAgencyIdKnown(); } else { resolve([]); }
                });
            };

            if (agencyIdNeeded) { if (!agencyIdKnown) { resolveAgencyId(); } else { onAgencyIdKnown() } } else { onAgencyIdKnown(); }
        });
    }
    else { return []; }
};

const getCurrentRoutes = async (
    gtfsAgencyId, agencyId, agencyIdInAgency, routeId, routeIdInAgency,
    includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON) => {
    return getRoutes(gtfsAgencyId, getCurrentTableName, undefined, agencyId, agencyIdInAgency, routeId, routeIdInAgency,
        includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON);
};

const getPublishedRoutes = async (
    gtfsAgencyId, onTransitDate, agencyId, agencyIdInAgency, routeId, routeIdInAgency,
    includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON) => {
    return getRoutes(gtfsAgencyId, getPublishedTableName, onTransitDate, agencyId, agencyIdInAgency, routeId, routeIdInAgency,
        includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON);
};

const getStops = async (gtfsAgencyId, getTableName, getTableNameDate, stopId, stopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters) => {
    let tableName = getTableName(gtfsAgencyId, GTFSModelsMap.stops, getTableNameDate);
    if (tableName) {
        return new Promise((resolve, reject) => {
            let isByDistance = (centerLon && centerLat);
            let stopSequenceIdStopIds;

            const onIdsKnown = () => {
                let sqlStr = isByDistance ? "SELECT ??,st_distance_sphere(??,?) as distance FROM ??" : "SELECT ?? FROM ??";
                let values = [stopsGTFSFieldNamesArray];
                if (isByDistance) { values.push(stopsFieldNamesMap.stop_point, db.getPointFrom(centerLon, centerLat)); }
                values.push(tableName);
                let andWhereStr = isByDistance ? " HAVING " : " WHERE ";
                if (stopId || stopIdInAgency) {
                    let stopIdUse = stopId ? stopId : stopIdInAgency;
                    if (stopIdUse.length !== undefined) { sqlStr += andWhereStr + "?? IN (?)"; } else { sqlStr += andWhereStr + "?? = ?"; }
                    if (stopId) { values.push.apply(values, [autoIncKeyColumnName, stopId]); }
                    else { values.push.apply(values, [stopsFieldNamesMap.stop_id, stopIdInAgency]); }
                    andWhereStr = " AND ";
                }
                if (stopSequenceIdStopIds) {
                    sqlStr += andWhereStr + "?? IN (?)";
                    values.push.apply(values, [autoIncKeyColumnName, stopSequenceIdStopIds]);
                    andWhereStr = " AND ";
                }
                if (isByDistance) {
                    if (!radiusInMeters) { radiusInMeters = gtfsDefaultStopDistanceRadiusInMeters; }
                    sqlStr += andWhereStr + "distance <= ?";
                    values.push(radiusInMeters);
                    andWhereStr = " AND ";
                }
                if (isByDistance) { sqlStr += " order by distance asc;"; }
                else {
                    sqlStr += orderByStrId;
                    values.push.apply(values, [stopsFieldNamesMap.stop_id, stopsFieldNamesMap.stop_id]);
                }
                return db.queryPoolValues(sqlStr, values).then(results => {
                    resolve(results);
                }).catch(err => { console.log('getStops: ' + err); resolve([]); });
            };

            const resolveStopSequenceIdStopIds = () => {
                if (stopSequenceId) {
                    getStopSequencesStops(
                        gtfsAgencyId, getTableName, getTableNameDate, stopSequenceId, true
                    ).then(results => {
                        if (results.length > 0) { stopSequenceIdStopIds = results.map(t => t[autoIncKeyColumnName]); }
                        onIdsKnown();
                    });
                } else { onIdsKnown(); }
            };

            resolveStopSequenceIdStopIds();
        });
    }
    else { return []; }
};

const getCurrentStops = async (gtfsAgencyId, stopId, stopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters) => {
    return getStops(gtfsAgencyId, getCurrentTableName, undefined, stopId, stopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters)
};

const getPublishedStops = async (gtfsAgencyId, onTransitDate, stopId, stopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters) => {
    return getStops(gtfsAgencyId, getPublishedTableName, onTransitDate, stopId, stopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters)
};

const getShapes = async (gtfsAgencyId, getTableName, getTableNameDate, shapeId, shapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    let tableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.shapes_compressed, getTableNameDate);
    if (tableName) {
        return new Promise((resolve, reject) => {
            let needsRouteId = (routeId || routeIdInAgency), knownRouteId = routeId;
            const onIdsKnown = () => {
                let sqlStr = "SELECT ?? FROM ??";
                let hasSimplified = includeOriginal ? !excludeSimplified : true;
                let fieldNamesMap = Object.assign({}, shapes_compressedFieldNamesMap);
                if (!hasSimplified) {
                    delete fieldNamesMap[shapes_compressedFieldNamesMap.shape_points];
                    delete fieldNamesMap[shapes_compressedFieldNamesMap.shape_dist_traveled];
                }
                if (!includeOriginal) {
                    delete fieldNamesMap[shapes_compressedFieldNamesMap.shape_original_points];
                    delete fieldNamesMap[shapes_compressedFieldNamesMap.shape_original_dist_traveled];
                }
                let fieldNamesArray = Object.keys(fieldNamesMap).map(key => fieldNamesMap[key]);
                let values = [fieldNamesArray, tableName];
                let whereAndStr = " WHERE ";
                if (shapeId || shapeIdInAgency) {
                    let shapeIdUse = shapeId ? shapeId : shapeIdInAgency;
                    if (shapeIdUse.length !== undefined) { sqlStr += whereAndStr + "?? IN (?)"; } else { sqlStr += whereAndStr + "?? = ?"; }
                    if (shapeId) { values.push.apply(values, [autoIncKeyColumnName, shapeId]); }
                    else { values.push.apply(values, [shapes_compressedFieldNamesMap.shape_id, shapeIdInAgency]); }
                    whereAndStr = " AND ";
                }
                if (routeId) {
                    let routesShapesTableName = getCurrentTableName(gtfsAgencyId, GTFSCustomModelsMap.routes_shapes);
                    sqlStr += whereAndStr + "?? in (SELECT ?? FROM ?? WHERE ??=?";
                    values.push.apply(values, [autoIncKeyColumnName, routes_shapesFieldNamesMap.shape_id, routesShapesTableName, routes_shapesFieldNamesMap.route_id, routeId]);
                    if (routeDirectionId !== undefined) {
                        sqlStr += " AND ??=?"
                        values.push.apply(values, [routes_shapesFieldNamesMap.direction_id, routeDirectionId]);
                    }
                    sqlStr += ")";
                }
                sqlStr += orderByStrId;
                values.push.apply(values, [shapes_compressedFieldNamesMap.shape_id, shapes_compressedFieldNamesMap.shape_id]);
                return db.queryPoolValues(sqlStr, values).then(results => {
                    let nResults = results.length;
                    if ((decodeData || returnGeoJSON) && nResults > 0) {
                        let polyCode = geom.PolyCode();
                        for (let i = 0; i < nResults; ++i) {
                            let r = results[i];
                            if (hasSimplified) {
                                r[shapes_compressedFieldNamesMap.shape_points] = getDecodeData(polyCode, r[shapes_compressedFieldNamesMap.shape_points], true, lineStringPrecision, returnGeoJSON);
                                r[shapes_compressedFieldNamesMap.shape_dist_traveled] = getDecodeData(polyCode, r[shapes_compressedFieldNamesMap.shape_dist_traveled], false, distancesPrecision, returnGeoJSON);
                            }
                            if (includeOriginal) {
                                r[shapes_compressedFieldNamesMap.shape_original_points] = getDecodeData(polyCode, r[shapes_compressedFieldNamesMap.shape_original_points], true, lineStringPrecision, returnGeoJSON);
                                r[shapes_compressedFieldNamesMap.shape_original_dist_traveled] = getDecodeData(polyCode, r[shapes_compressedFieldNamesMap.shape_original_dist_traveled], false, distancesPrecision, returnGeoJSON);
                            }
                        }
                    }
                    resolve(results);
                }).catch(err => { console.log('getShapes: ' + err); resolve([]); });
            };

            const resolveRouteId = () => {
                if (needsRouteId && !knownRouteId) {
                    getRouteByRouteAgencyId(gtfsAgencyId, getTableName, getTableNameDate, routeIdInAgency).then(results => { if (results) { routeId = results[autoIncKeyColumnName]; onIdsKnown(); } else { resolve([]); } });
                }
                else { onIdsKnown(); }
            };

            resolveRouteId();
        });
    }
    else { return []; }
};

const getCurrentShapes = async (gtfsAgencyId, shapeId, shapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getShapes(gtfsAgencyId, getCurrentTableName, undefined, shapeId, shapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON);
};

const getPublishedShapes = async (gtfsAgencyId, onTransitDate, shapeId, shapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getShapes(gtfsAgencyId, getPublishedTableName, onTransitDate, shapeId, shapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON);
};

const getCalendar = async (gtfsAgencyId, getTableName, getTableNameDate, serviceId, serviceIdInAgency, onDate) => {
    let tableName = getTableName(gtfsAgencyId, GTFSModelsMap.calendar, getTableNameDate);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ??";
        let values = [calendarFieldNamesArray, tableName];
        let andWhereStr = " WHERE ";
        if (serviceId || serviceIdInAgency) {
            let serviceIdUse = serviceId ? serviceId : serviceIdInAgency;
            if (serviceIdUse.length !== undefined) { sqlStr += andWhereStr + "?? IN (?)"; } else { sqlStr += andWhereStr + "?? = ?"; }
            if (serviceId) { values.push.apply(values, [autoIncKeyColumnName, serviceId]); }
            else { values.push.apply(values, [calendarFieldNamesMap.service_id, serviceIdInAgency]); }
            andWhereStr = " AND ";
        }
        if (onDate) {
            let calendarDatesTableName = getTableName(gtfsAgencyId, GTFSModelsMap.calendar_dates, getTableNameDate);
            let onDateDate = db.YYYYMMDDToDate(onDate);
            let dowMask = gtfsGetDayMaskFromDate(onDateDate);

            sqlStr += andWhereStr + "((((?? <= ? or ?? is null) and ";
            sqlStr += "(?? >= ? or ?? is null) ";
            sqlStr += "and(?? = 0 or (?? & ?) = ?))";
            sqlStr += "or ((?? in (SELECT ?? from ?? where ?? = ? and ?? = ?)))))";
            sqlStr += "and (?? not in (SELECT ?? from ?? where ?? = ? and ?? = ?))";

            let mc = calendarFieldNamesMap;
            let mcd = calendar_datesFieldNamesMap;

            values.push.apply(values, [
                mc.start_date_date, onDateDate, mc.start_date_date,
                mc.end_date_date, onDateDate, mc.end_date_date,
                mc.wd_mask, mc.wd_mask, dowMask, dowMask,
                autoIncKeyColumnName, mcd.service_id, calendarDatesTableName, mcd.date_date, onDateDate, mcd.exception_type, gtfsCalendarDatesExceptionTypeAvailable,
                autoIncKeyColumnName, mcd.service_id, calendarDatesTableName, mcd.date_date, onDateDate, mcd.exception_type, gtfsCalendarDatesExceptionTypeUnavailable
            ]);

            andWhereStr = " AND ";
        }
        sqlStr += orderByStrId;
        values.push.apply(values, [calendarFieldNamesMap.service_id, calendarFieldNamesMap.service_id]);
        return db.queryPoolValues(sqlStr, values).then(results => {
            return results;
        }).catch(err => { console.log('getCurrentCalendars: ' + err); return []; });
    }
    else { return []; }
};

const getCurrentCalendar = async (gtfsAgencyId, serviceId, serviceIdInAgency, onDate) => {
    return getCalendar(gtfsAgencyId, getCurrentTableName, undefined, serviceId, serviceIdInAgency, onDate);
};

const getPublishedCalendar = async (gtfsAgencyId, onTransitDate, serviceId, serviceIdInAgency, onDate) => {
    return getCalendar(gtfsAgencyId, getPublishedTableName, onTransitDate, serviceId, serviceIdInAgency, onDate);
};

const getCalendarDates = async (gtfsAgencyId, getTableName, getTableNameDate, serviceId, serviceIdInAgency, onDate) => {
    let tableName = getTableName(gtfsAgencyId, GTFSModelsMap.calendar_dates, getTableNameDate);
    if (tableName) {
        let sqlStr = "SELECT ?? FROM ??";
        let values = [calendar_datesFieldNamesArray, tableName];
        let whereAndStr = " WHERE ";
        if (serviceId || serviceIdInAgency) {
            let serviceIdUse = serviceId ? serviceId : serviceIdInAgency;
            if (serviceIdUse.length !== undefined) { sqlStr += whereAndStr + "?? IN (?)"; } else { sqlStr += whereAndStr + "?? = ?"; }
            if (serviceId) { values.push.apply(values, [calendar_datesFieldNamesMap.service_id, serviceId]); }
            else { values.push.apply(values, [calendar_datesFieldNamesMap.service_id_in_agency, serviceIdInAgency]); }
            whereAndStr = " AND ";
        }
        if (onDate) {
            sqlStr += whereAndStr + "??=?"
            values.push.apply(values, [calendar_datesFieldNamesMap.date_date, onDate]);
        }
        sqlStr += orderByStrId;
        values.push.apply(values, [calendar_datesFieldNamesMap.service_id, calendar_datesFieldNamesMap.service_id]);
        return db.queryPoolValues(sqlStr, values).then(results => {
            return results;
        }).catch(err => { console.log('getCurrentCalendarDates: ' + err); return []; });
    }
    else { return []; }
};

const getCurrentCalendarDates = async (gtfsAgencyId, serviceId, serviceIdInAgency, onDate) => { return getCalendarDates(gtfsAgencyId, getCurrentTableName, undefined, serviceId, serviceIdInAgency, onDate); };

const getPublishedCalendarDates = async (gtfsAgencyId, onTransitDate, serviceId, serviceIdInAgency, onDate) => { return getCalendarDates(gtfsAgencyId, getPublishedTableName, onTransitDate, serviceId, serviceIdInAgency, onDate); };

const getServices = async (gtfsAgencyId, getTableName, getTableNameDate, serviceId, serviceIdInAgency, onDate) => {
    let currentCalendarResult, currentCalendarResultByServiceId;
    return getCalendar(
        gtfsAgencyId, getTableName, getTableNameDate, serviceId, serviceIdInAgency, onDate
    ).then(result => {
        currentCalendarResult = result;
        let reduceResults = currentCalendarResult.reduce((allMap, cur, index) => {
            cur.calendar_dates = [];
            let serviceId = cur[autoIncKeyColumnName];//cur[calendarFieldNamesMap.service_id];
            allMap.indices['' + serviceId] = index;
            allMap.ids.push(serviceId);
            return allMap;
        }, { indices: {}, ids: [] });
        currentCalendarResultByServiceId = reduceResults.indices;
        serviceId = reduceResults.ids;
        return getCalendarDates(
            gtfsAgencyId, getTableName, getTableNameDate, serviceId, undefined, onDate
        );
    }).then(result => {
        let serviceIndex = 0;
        for (let i in result) {
            let cdr = result[i];
            let cdrServiceId = cdr[calendar_datesFieldNamesMap.service_id];
            let cdi = currentCalendarResultByServiceId['' + cdrServiceId];
            if (cdi !== undefined) {
                let cd = currentCalendarResult[cdi];
                cd.calendar_dates.push(cdr);
            }
        }
        return currentCalendarResult;
    });
};

const getCurrentServices = async (gtfsAgencyId, serviceId, serviceIdInAgency, onDate) => { return getServices(gtfsAgencyId, getCurrentTableName, undefined, serviceId, serviceIdInAgency, onDate); };

const getPublishedServices = async (gtfsAgencyId, onTransitDate, serviceId, serviceIdInAgency, onDate) => { return getServices(gtfsAgencyId, getPublishedTableName, onTransitDate, serviceId, serviceIdInAgency, onDate); };

const getStopSequences = async (gtfsAgencyId, getTableName, getTableNameDate, stopSequenceId, routeId, routeIdInAgency, routeDirectionId, stopId, stopIdInAgency, decodeData) => {
    let tableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stop_sequences, getTableNameDate);
    if (tableName) {
        return new Promise((resolve, reject) => {
            let needsRouteId = (routeId || routeIdInAgency);
            let needsStopId = (stopId || stopIdInAgency);
            let routeIdKnown = !!routeId, stopIdKnown = !!stopId;

            const onIdsKnown = () => {
                let sqlStr = "SELECT ?? FROM ??";
                let whereAndStr = " WHERE ";
                let whereAndEndIn = "?? IN (SELECT ?? FROM ?? WHERE ??=?)";
                let values = [stop_sequencesFieldNamesArray, tableName];
                if (stopSequenceId) {
                    if (stopSequenceId.length !== undefined) { sqlStr += whereAndStr + "?? IN (?)"; } else { sqlStr += whereAndStr + "?? = ?"; }
                    values.push.apply(values, [autoIncKeyColumnName, stopSequenceId]);
                    whereAndStr = " AND ";
                }
                if (stopId) {
                    let stopsTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stops_sseqs, getTableNameDate);
                    sqlStr += whereAndStr + whereAndEndIn;
                    values.push.apply(values, [autoIncKeyColumnName, stops_sseqsFieldNamesMap.sseq_id, stopsTableName, stops_sseqsFieldNamesMap.stop_id, stopId]);
                    whereAndStr = " AND ";
                }
                if (routeId) {
                    let routesSSeqsTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.routes_sseqs, getTableNameDate);
                    //sqlStr += whereAndStr + whereAndEndIn;
                    if (routeDirectionId === undefined) {
                        sqlStr += whereAndStr + whereAndEndIn;
                        values.push.apply(values, [autoIncKeyColumnName, routes_sseqsFieldNamesMap.sseq_id, routesSSeqsTableName, routes_sseqsFieldNamesMap.route_id, routeId]);
                    }
                    else {
                        sqlStr += whereAndStr + "?? IN (SELECT ?? FROM ?? WHERE ??=? AND ??=?)";
                        values.push.apply(values, [autoIncKeyColumnName, routes_sseqsFieldNamesMap.sseq_id, routesSSeqsTableName, routes_sseqsFieldNamesMap.route_id, routeId, routes_sseqsFieldNamesMap.direction_id, routeDirectionId]);
                    }
                    whereAndStr = " AND ";
                }
                //sqlStr += orderByStrId;
                //values.push.apply(values, [autoIncKeyColumnName, autoIncKeyColumnName]);
                sqlStr += orderByTwoKeysStrId;
                values.push.apply(values, [stop_sequencesFieldNamesMap.route_id_in_agency, stop_sequencesFieldNamesMap.route_id_in_agency, stop_sequencesFieldNamesMap.trip_headsign, stop_sequencesFieldNamesMap.trip_headsign]);
                return db.queryPoolValues(sqlStr, values).then(results => {
                    if (decodeData) { results = decodeStopSequences(results); }
                    let nResults = results.length;
                    for (let i = 0; i < nResults; ++i) {
                        let r = results[i];
                        let gtfs_direction = r[stop_sequencesFieldNamesMap.gtfs_direction];
                        r[gtfs_direction_name_FieldName] = gtfsDirectionFriendlyNamesArray[gtfs_direction];
                    }
                    resolve(results);
                }).catch(err => { console.log('getCurrentStopSequences: ' + err); resolve([]); });
            };

            const resolveStopId = () => {
                if (needsStopId && !stopIdKnown) {
                    getStopByStopAgencyId(gtfsAgencyId, getTableName, getTableNameDate, stopIdInAgency).then(result => { if (result) { stopId = result[autoIncKeyColumnName]; onIdsKnown(); } else { resolve([]); } });
                }
                else { onIdsKnown(); }
            };

            const resolveRouteId = () => {
                if (needsRouteId && !routeIdKnown) {
                    getRouteByRouteAgencyId(gtfsAgencyId, getTableName, getTableNameDate, routeIdInAgency).then(results => { if (results) { routeId = results[autoIncKeyColumnName]; resolveStopId(); } else { resolve([]); } });
                }
                else { resolveStopId(); }
            };

            resolveRouteId();
        });
    }
    else { return []; }
};

const getPublishedStopSequences = async (gtfsAgencyId, onTransitDate, stopSequenceId, routeId, routeIdInAgency, routeDirectionId, stopId, stopIdInAgency, decodeData) => {
    return getStopSequences(gtfsAgencyId, getPublishedTableName, onTransitDate, stopSequenceId, routeId, routeIdInAgency, routeDirectionId, stopId, stopIdInAgency, decodeData);
};

const getCurrentStopSequences = async (gtfsAgencyId, stopSequenceId, routeId, routeIdInAgency, routeDirectionId, stopId, stopIdInAgency, decodeData) => {
    return getStopSequences(gtfsAgencyId, getCurrentTableName, undefined, stopSequenceId, routeId, routeIdInAgency, routeDirectionId, stopId, stopIdInAgency, decodeData);
};

const getTripStopSequences = async (gtfsAgencyId, getTableName, getTableNameDate, tripIds, decodeData) => {
    let stopSequencesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stop_sequences, getTableNameDate);
    let tripSequencesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.trips_sseqs, getTableNameDate);
    let sqlStr = "select * from ?? where ?? in (SELECT distinct ?? FROM ?? where ?? in (?));"
    if (tripIds.length === undefined) { tripIds = [tripIds]; }
    let values = [stopSequencesTableName, autoIncKeyColumnName, trips_sseqsFieldNamesMap.sseq_id, tripSequencesTableName, trips_sseqsFieldNamesMap.trip_id, tripIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        if (decodeData) { results = decodeStopSequences(results); }
        return results;
    }).catch(err => { console.log('getTripStopSequences: ' + err); throw (err); });
};

const getTripStopDistances = async (gtfsAgencyId, getTableName, getTableNameDate, tripIds, decodeData) => {
    let stopDistancesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stop_distances, getTableNameDate);
    let tripDistancesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.trips_sseqs, getTableNameDate);
    let sqlStr = "select * from ?? where ?? in (SELECT distinct ?? FROM ?? where ?? in (?));"
    if (tripIds.length === undefined) { tripIds = [tripIds]; }
    let values = [stopDistancesTableName, autoIncKeyColumnName, trips_sseqsFieldNamesMap.sdist_id, tripDistancesTableName, trips_sseqsFieldNamesMap.trip_id, tripIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        if (decodeData) { results = decodeStopDistances(results); }
        return results;
    }).catch(err => { console.log('getTripStopDistances: ' + err); throw (err); });
};

const getTripStopTimes = async (gtfsAgencyId, getTableName, getTableNameDate, tripIds, decodeData) => {
    let stopTimesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stop_hms_offsets, getTableNameDate);
    let tripStopTimesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.trips_sseqs, getTableNameDate);
    let sqlStr = "select * from ?? where ?? in (SELECT distinct ?? FROM ?? where ?? in (?));"
    if (tripIds.length === undefined) { tripIds = [tripIds]; }
    let values = [stopTimesTableName, autoIncKeyColumnName, trips_sseqsFieldNamesMap.stimes_id, tripStopTimesTableName, trips_sseqsFieldNamesMap.trip_id, tripIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        return decodeStopTimes(results, decodeData);
    }).catch(err => { console.log('getTripStopTimes: ' + err); throw (err); });
};

const getStopSequencesStops = async (gtfsAgencyId, getTableName, getTableNameDate, stopSequenceIds, decodeData) => {
    let stopsSequencesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stops_sseqs, getTableNameDate);
    let stopsTableName = getTableName(gtfsAgencyId, GTFSModelsMap.stops, getTableNameDate);
    let sqlStr = "SELECT ?? FROM ?? WHERE ?? IN (SELECT ?? FROM ?? WHERE ?? IN (?))";
    if (stopSequenceIds.length === undefined) { stopSequenceIds = [stopSequenceIds]; }
    let values = [stopsGTFSFieldNamesArray, stopsTableName, autoIncKeyColumnName, stops_sseqsFieldNamesMap.stop_id, stopsSequencesTableName, stops_sseqsFieldNamesMap.sseq_id, stopSequenceIds];
    return db.queryPoolValues(sqlStr, values).then(results => {
        //if (decodeData) { results = decodeStopSequenceStops(results); }
        return results;
    }).catch(err => { console.log('getStopSequencesStops: ' + err); throw (err); });
};

const getTrips = async (gtfsAgencyId, getTableName, getTableNameDate,
    tripId, tripIdInAgency, routeId, routeIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, stopSequenceId,
    stopIds, stopIdsInAgency,
    onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
    includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {

    let tableName = getTableName(gtfsAgencyId, GTFSModelsMap.trips, getTableNameDate);

    if (tableName && (routeId || routeIdInAgency || tripId || tripIdInAgency || stopSequenceId || stopIds || stopIdsInAgency || onDate)) {
        return new Promise((resolve, reject) => {
            let needsRouteId = (routeId || routeIdInAgency), knownRouteId = routeId;
            let needsServiceIds = (serviceIds || serviceIdsInAgency), knownServiceIds = serviceIds;
            let needsStopIds = (stopIds || stopIdsInAgency), knownStopIds = stopIds;
            let tripResults, shapeResults, stopSequenceResults, stopTimesResults, stopDistancesResults, stopsResults, routesResults;
            let tripResultsShapeIds, tripResultsRouteIds;
            let polyCode;
            let onDateServiceIds;

            const getPolyCode = () => { return polyCode ? polyCode : (polyCode = geom.PolyCode()); };

            const onResolve = () => {
                let nTrips = tripResults ? tripResults.length : 0;
                let nStopTimes = (includeStopTimes && stopTimesResults) ? stopTimesResults.length : undefined;
                let nStopSequences = (includeStopSequences && stopSequenceResults) ? stopSequenceResults.length : undefined;
                let nStopDistances = (includeStopDistances && stopDistancesResults) ? stopDistancesResults.length : undefined;
                let nStops = (includeStops && stopsResults) ? stopsResults.length : undefined;
                let nRoutes = (includeRoutes && routesResults) ? routesResults.length : undefined;
                let nShapes = (includeShapes && shapeResults) ? shapeResults.length : undefined;

                resolve({
                    nRoutes: nRoutes,
                    nStops: nStops,
                    nStopSequences: nStopSequences,
                    nStopTimes: nStopTimes,
                    nStopDistances: nStopDistances,
                    nShapes: nShapes,
                    nTrips: nTrips,
                    routes: routesResults,
                    stops: stopsResults,
                    stopSequences: stopSequenceResults,
                    stopTimes: stopTimesResults,
                    stopDistances: stopDistancesResults,
                    shapes: shapeResults,
                    trips: tripResults
                });
            };

            const onIncludeRoutes = () => {
                if (includeRoutes) {
                    let includeRouteDirections = false;
                    let includeRouteDirectionShape = false;
                    let includeRouteShape = false;
                    getRoutes(
                        gtfsAgencyId, getTableName, getTableNameDate, undefined, undefined, tripResultsRouteIds, undefined, includeRouteDirections, includeRouteDirectionShape, includeRouteShape, decodeData, returnGeoJSON
                    ).then(results => {
                        routesResults = results;
                        onResolve();
                    }).catch(err => {
                        onResolve();
                    });
                }
                else { onResolve(); }
            };

            const onIncludeStopDistances = () => {
                if (includeStopDistances) {
                    getTripStopDistances(
                        gtfsAgencyId, getTableName, getTableNameDate, tripId, decodeData
                    ).then(results => {
                        stopDistancesResults = results;
                        onIncludeRoutes();
                    }).catch(err => {
                        onIncludeRoutes();
                    });
                }
                else { onIncludeRoutes(); }
            };

            const onIncludeStopTimes = () => {
                if (includeStopTimes) {
                    getTripStopTimes(
                        gtfsAgencyId, getTableName, getTableNameDate, tripId, decodeData
                    ).then(results => { stopTimesResults = results; onIncludeStopDistances(); }).catch(err => { onIncludeStopDistances(); });
                }
                else { onIncludeStopDistances(); }
            };

            const onIncludeStopSequences = () => {
                if (includeStopSequences || includeStops) {
                    getTripStopSequences(
                        gtfsAgencyId, getTableName, getTableNameDate, tripId, decodeData
                    ).then(results => {
                        stopSequenceResults = results;
                        let nResults = stopSequenceResults.length;
                        if (nResults > 0 && includeStops) {
                            let stopSequenceIds = stopSequenceResults.reduce((mapAndIds, cur) => {
                                let stopSequenceId = cur[autoIncKeyColumnName], key = '' + stopSequenceId;
                                if (!mapAndIds.idsMap[key]) { mapAndIds.ids.push(mapAndIds.idsMap[key] = stopSequenceId); }
                                return mapAndIds;
                            }, { ids: [], idsMap: {} }).ids;
                            getStopSequencesStops(
                                gtfsAgencyId, getTableName, getTableNameDate, stopSequenceIds, decodeData
                            ).then(results => {
                                stopsResults = results;
                                if (!includeStopSequences) { stopSequenceResults = undefined; }
                                onIncludeStopTimes();
                                }).catch(err => {
                                    if (!includeStopSequences) { stopSequenceResults = undefined; }
                                    onIncludeStopTimes();
                                });
                        }
                        else {
                            if (!includeStopSequences) { stopSequenceResults = undefined; }
                            onIncludeStopTimes();
                        }
                    }).catch(err => {
                        onIncludeStopTimes();
                    });
                }
                else { onIncludeStopTimes(); }
            };

            const onIncludeShapes = () => {
                if (includeShapes) {
                    return getShapes(
                        gtfsAgencyId, getTableName, getTableNameDate, tripResultsShapeIds, undefined, undefined, undefined, undefined, includeOriginal, excludeSimplified, decodeData, returnGeoJSON
                    ).then(results => { shapeResults = results; onIncludeStopSequences(); });
                }
                else { onIncludeStopSequences(); }
            };

            const onIdsKnown = () => {
                let tripSequencesTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.trips_sseqs, getTableNameDate);

                let sqlStr = "SELECT ";
                let fieldCount = tripsFieldNamesArray.length + trips_sseqsFieldNamesArray.length - 1;
                for (let i = 0; i < fieldCount; ++i) { sqlStr += "??.??"; if (i < fieldCount - 1) { sqlStr += ','; } }
                sqlStr += " FROM ??,?? WHERE ??.??=??.??"
                let whereAndStr = " AND ";
                let fieldNames = tripsFieldNamesArray.reduce((prev, cur) => { prev.push(tableName, cur); return prev; }, []);
                let otherFieldNames = trips_sseqsFieldNamesArray.slice(1).reduce((prev, cur) => { prev.push(tripSequencesTableName, cur); return prev; }, []);
                fieldNames.push.apply(fieldNames, otherFieldNames);
                let values = [];
                values.push.apply(values, fieldNames);
                values.push.apply(values, [tableName, tripSequencesTableName, tableName, tripsFieldNamesMap.id, tripSequencesTableName, trips_sseqsFieldNamesMap.trip_id]);

                if (needsRouteId) {
                    sqlStr += whereAndStr + "??.??=?";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tableName, tripsFieldNamesMap.route_id, routeId]);
                    if (routeDirectionId !== undefined) {
                        sqlStr += whereAndStr + "??.??=?";
                        values.push.apply(values, [tableName, tripsFieldNamesMap.direction_id, routeDirectionId]);
                    }
                }
                if (tripId || tripIdInAgency) {
                    let tripIdUse = tripId ? tripId : tripIdInAgency;
                    if (tripIdUse.length !== undefined) { sqlStr += whereAndStr + "?? IN (?)"; } else { sqlStr += whereAndStr + "??=?"; }
                    if (tripId) { values.push.apply(values, [autoIncKeyColumnName, tripId]); }
                    else { values.push.apply(values, [tripsFieldNamesMap.trip_id, tripIdInAgency]); }
                    whereAndStr = " AND ";
                }
                if (serviceIds) {
                    sqlStr += whereAndStr + "?? IN (?)";
                    values.push.apply(values, [tripsFieldNamesMap.service_id, serviceIds]);
                    whereAndStr = " AND ";
                }
                if (onDateServiceIds) {
                    sqlStr += whereAndStr + "?? IN (?)";
                    values.push.apply(values, [tripsFieldNamesMap.service_id, onDateServiceIds]);
                    whereAndStr = " AND ";
                }
                if (stopSequenceId) {
                    sqlStr += whereAndStr + "??.?? IN (SELECT ?? FROM ?? WHERE ??=?)";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tableName, autoIncKeyColumnName, trips_sseqsFieldNamesMap.trip_id, tripSequencesTableName, trips_sseqsFieldNamesMap.sseq_id, stopSequenceId]);
                }
                if (stopIds) {
                    //let stopsSseqsTableName = getCurrentTableName(gtfsAgencyId, GTFSCustomModelsMap.stops_sseqs);
                    let stopsSseqsTableName = getTableName(gtfsAgencyId, GTFSCustomModelsMap.stops_sseqs, getTableNameDate);
                    sqlStr += whereAndStr + "??.?? IN (SELECT ?? FROM ?? WHERE ?? IN(SELECT ?? FROM ?? WHERE ?? IN(?)))";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tableName, autoIncKeyColumnName, trips_sseqsFieldNamesMap.trip_id, tripSequencesTableName, trips_sseqsFieldNamesMap.sseq_id,
                        stops_sseqsFieldNamesMap.sseq_id, stopsSseqsTableName, stops_sseqsFieldNamesMap.stop_id, stopIds]);
                }
                if (minStartHMS) {
                    sqlStr += whereAndStr + "??.??>=?";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tripSequencesTableName, trips_sseqsFieldNamesMap.start_hms, minStartHMS]);
                }
                if (maxStartHMS) {
                    sqlStr += whereAndStr + "??.??<=?";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tripSequencesTableName, trips_sseqsFieldNamesMap.start_hms, maxStartHMS]);
                }
                if (minEndHMS) {
                    sqlStr += whereAndStr + "??.??>=?";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tripSequencesTableName, trips_sseqsFieldNamesMap.end_hms, minEndHMS]);
                }
                if (maxEndHMS) {
                    sqlStr += whereAndStr + "??.??<=?";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tripSequencesTableName, trips_sseqsFieldNamesMap.end_hms, maxEndHMS]);
                }
                if (routeTypeList) {
                    sqlStr += whereAndStr + "??.?? IN(?)";
                    whereAndStr = " AND ";
                    values.push.apply(values, [tableName, tripsFieldNamesMap.route_type, routeTypeList]);
                }
                sqlStr += orderByTableQualifiedStrId;
                values.push.apply(values, [tableName, tripsFieldNamesMap.trip_id, tableName, tripsFieldNamesMap.trip_id]);
                return db.queryPoolValues(sqlStr, values).then(results => {
                    tripResults = results;
                    let nTripResults = tripResults.length;
                    if (nTripResults > 0) {
                        let reduced = tripResults.reduce((reducedResults, cur) => {
                            let id = cur[autoIncKeyColumnName], idKey = '' + id;
                            let shapeId = cur[tripsFieldNamesMap.shape_id], shapeKey = '' + shapeId;
                            let routeId = cur[tripsFieldNamesMap.route_id], routeKey = '' + routeId;
                            if (!reducedResults.idsMap[idKey]) { reducedResults.ids.push(reducedResults.idsMap[idKey] = id); }
                            if (shapeId && !reducedResults.shapeIdsMap[shapeKey]) { reducedResults.shapeIds.push(reducedResults.shapeIdsMap[shapeKey] = shapeId); }
                            if (routeId && !reducedResults.routeIdsMap[routeKey]) { reducedResults.routeIds.push(reducedResults.routeIdsMap[routeKey] = routeId); }
                            return reducedResults;
                        }, { ids: [], idsMap: {}, shapeIds: [], shapeIdsMap: {}, routeIds: [], routeIdsMap: {} });
                        tripResultsShapeIds = reduced.shapeIds;
                        tripResultsRouteIds = reduced.routeIds;
                        tripId = reduced.ids;
                        onIncludeShapes();
                    }
                    else { onResolve(); }
                }).catch(err => { console.log('getTrips: ' + err); onResolve(); });
            };

            const resolveStopIds = () => {
                if (needsStopIds && !knownStopIds) {
                    getStopsByStopAgencyId(gtfsAgencyId, getTableName, getTableNameDate, stopIdsInAgency).then(results => {
                        if (results) { stopIds = results.map(t => t[autoIncKeyColumnName]); onIdsKnown(); }
                        else { onResolve(); }
                    });
                } else { onIdsKnown(); }
            };

            const resolveServiceIds = () => {
                if (needsServiceIds && !knownServiceIds) {
                    getServicesByServiceAgencyId(gtfsAgencyId, getTableName, getTableNameDate, serviceIdsInAgency).then(results => {
                        if (results) { serviceIds = results.map(t => t[autoIncKeyColumnName]); resolveStopIds(); }
                        else { onResolve(); }
                    });
                } else { resolveStopIds(); }
            };

            const resolveRouteId = () => {
                if (needsRouteId && !knownRouteId) {
                    getRouteByRouteAgencyId(gtfsAgencyId, getTableName, getTableNameDate, routeIdInAgency).then(results => { if (results) { routeId = results[autoIncKeyColumnName]; resolveServiceIds(); } else { resolve([]); } });
                }
                else { resolveServiceIds(); }
            };

            const resolveOnDate = () => {
                if (onDate) {
                    getServices(
                        gtfsAgencyId, getTableName, getTableNameDate, undefined, undefined, onDate
                    ).then(results => {
                        if (results) { onDateServiceIds = results.map(t => t[autoIncKeyColumnName]); resolveRouteId(); }
                        else { onResolve(); }
                    });
                }
                else { resolveRouteId(); }
            };

            resolveOnDate();
        });
    }
    else { return []; }
};

const getCurrentTrips = async (gtfsAgencyId,
    tripId, tripIdInAgency, routeId, routeIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, stopSequenceId,
    stopIds, stopIdsInAgency,
    onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
    includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getTrips(gtfsAgencyId, getCurrentTableName, undefined, tripId, tripIdInAgency, routeId, routeIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, stopSequenceId,
        stopIds, stopIdsInAgency,
        onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
        includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON);
};

const getPublishedTrips = async (gtfsAgencyId, onTransitDate,
    tripId, tripIdInAgency, routeId, routeIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, stopSequenceId,
    stopIds, stopIdsInAgency,
    onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
    includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getTrips(gtfsAgencyId, getPublishedTableName, onTransitDate, tripId, tripIdInAgency, routeId, routeIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, stopSequenceId,
        stopIds, stopIdsInAgency,
        onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
        includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON);
};

const GTFSImport = function(settings) {
    let theThis; if (!((theThis = this) instanceof GTFSImport)) { return new GTFSImport(settings); }
    let importInfo, records, allTableSpecs, hasCalendarFile, defaultCalendarRecord, preProcessLine;
    let polyCode, lineStringPrecision, distancesPrecision, lineStringSimplifyTolerance, shapeObjs, shapeIdsByAutoIncKey, shapeIdCounter, cShapesById;
    let curShapeRecord, curShapeSeq;
    let importId, notifyOrder;
    let stopSequenceKeys, nStopSequenceKeys;
    let stopDistanceKeys, nStopDistanceKeys;
    let stopTimeOffsetsKeys, nStopTimeOffsetsKeys;
    let agencyIdsByAutoIncIds, autoIncIdCountersTable;
    let globalDistanceRatio;
    let inStationObjs, nInStationObjs;
    let agencyExtent;

    this.Import = () => {
        let miliStart = Date.now();
        initImport();
        return Promise.each(GTFSModels.slice(0).reverse(), t => {
            return db.dropTableIfExists(getTempTableName(settings.gtfsAgencyId, t));
        }).then(() => {
            return Promise.each(GTFSCustomModels.slice(0).reverse(), t => {
                return db.dropTableIfExists(getTempTableName(settings.gtfsAgencyId, t));
            });
        }).then(() => {
            return importGTFSFiles();
        }).catch(err => {
            return err;
        }).then(err => {
            return err ? err : createCustomGTFS();
        }).then(err => {
            importInfo.success = err === undefined;
            if (err) { importInfo.err = err; }
            importInfo.millis = Date.now() - miliStart;
            importInfo.secs = Math.round(importInfo.millis / 1000);
            if (importInfo.success) {
                if (importInfo.prevIsIdenticalToCurrent) {
                    addMessage('no changes were detected; agency was not updated', true);
                    //return true;
                    return dropTempTables(settings.gtfsAgencyId).then(() => { return dropBakTables(settings.gtfsAgencyId); });
                }
                else {
                    addMessage('updating current agency');
                    return checkAgencyHasCurrentSet(
                        settings.gtfsAgencyId
                    ).then(exists => {
                        //return exists;
                        return renameTables(exists);
                    });
                }
            }
            else {
                //return true;
                return dropTempTables(settings.gtfsAgencyId).then(() => { return dropBakTables(settings.gtfsAgencyId); });
            }
        }).then(finalStatus => {
            let importNSecs = ' (' + importInfo.secs + 's)';
            if (importInfo.err) {
                addMessage('import error: ' + importInfo.err.message + importNSecs, false, true);
            }
            else {
                addMessage('import complete' + importNSecs);
            }
            return finalStatus;
        }).then(finalStatus => {
            if (!importInfo.prevIsIdenticalToCurrent) { getAgency().notifyAgencyChanged(settings.gtfsAgencyId); }
            return importInfo;
        });
    };

    const initImport = () => {
        agencyExtent = undefined;
        globalDistanceRatio = undefined;
        cShapesById = {};
        shapeIdCounter = 0;
        importId = ++globalImportId;
        notifyOrder = 0;
        stopSequenceKeys = {};
        nStopSequenceKeys = 0;
        stopDistanceKeys = {};
        nStopDistanceKeys = 0;
        stopTimeOffsetsKeys = {};
        nStopTimeOffsetsKeys = 0;
        shapeObjs = {};
        shapeIdsByAutoIncKey = {};
        agencyIdsByAutoIncIds = {};
        allTableSpecs = {};
        importInfo = { messages: [], prevIsIdenticalToCurrent: false };
        records = {};
        autoIncIdCountersTable = {};
        inStationObjs = {};
        nInStationObjs = 0;
        hasCalendarFile = true;
        for (let i in csvFileNamesArray) {
            let fileName = csvFileNamesArray[i];
            importInfo[fileName] = { addedRecords: 0 };
            records[fileName] = {};
            autoIncIdCountersTable[fileName] = 0;
            agencyIdsByAutoIncIds[fileName] = {};
        }
        for (let i in customGTFSNamesArray) {
            let fileName = customGTFSNamesArray[i];
            importInfo[fileName] = { addedRecords: 0 };
            records[fileName] = {};
            autoIncIdCountersTable[fileName] = 0;
        }
        defaultCalendarRecord = createDefaultCalendarRecord();
        addMessage('import started');
    };

    const addToExtent = coords => { return agencyExtent = geom.updateMapExtent(agencyExtent, coords); };

    const ImportGTFSFiles = function(importSettings) {
        let theThis; if (!((theThis = this) instanceof ImportGTFSFiles)) { return new ImportGTFSFiles(importSettings); }
        let index;

        const importNext = () => {
            let importFiles = GTFSModels.filter(t => { return t.io == index; });
            //console.log('importing: ' + index);
            ++index;
            if (importFiles.length) {
                let importPromises = importFiles.map(t => { return processGTFSModel(t) });
                return Promise.all(importPromises).then(result => { importNext(); }).catch(err => { importSettings.cb(err); });
            }
            else { importSettings.cb(); }
        };

        const initialize = () => { index = 0; importNext(); };

        initialize();
    };

    const importGTFSFiles = async () => {
        return new Promise((resolve, reject) => {
            ImportGTFSFiles({ cb: (err, result) => { if (err) { reject(err); } else { resolve(result); } } });
        });
    };

    const CustomGTFSCreator = function(creatorSettings) {
        let theThis; if (!((theThis = this) instanceof CustomGTFSCreator)) { return new CustomGTFSCreator(creatorSettings); }
        let creators, index;

        const createNext = () => {
            if (index < creators.length) {
                return Promise.resolve(
                    creators[index++]()
                ).reflect(
                    ).then(inspection => {
                        if (!inspection.isFulfilled()) { creatorSettings.cb(inspection.reason()); }
                        else { createNext(); }
                    });
            }
            else {
                creatorSettings.cb();
            }
        };

        const initialize = () => {
            creators = [addTripsIndices, updateStations, createCShapes, createStopSequences, createCurrentInfo, doCompareTempAndCurrentAgency];
            index = 0;
            createNext();
        };

        initialize();
    };

    const createCustomGTFS = async () => {
        return Promise.resolve(
            new Promise((resolve, reject) => { CustomGTFSCreator({ cb: (err, result) => { if (err) { reject(err) } else { resolve(result); } } });
        })).reflect().then(inspection => { return !inspection.isFulfilled() ? inspection.reason() : undefined; });
    };

    /*const renameTablesInModels = async (models, oldSetExists) => {
        let getBakName = oldSetExists ? getBakTableName : undefined;
        let sql = makeRenameSql(settings.gtfsAgencyId, models, getTempTableName, getBakName, getCurrentTableName);
        return db.queryPoolVoid(sql).then(() => { return true; }).catch(err => { console.log('renameTables: ' + err.message); return false; });
    };

    const renameTables = async oldSetExists => {
        return dropBakTables(
            settings.gtfsAgencyId
        ).then(() => {
            return renameTablesInModels(GTFSModels, oldSetExists);
        }).then(() => {
            return renameTablesInModels(GTFSCustomModels, oldSetExists);
        }).then(status => {
            return dropBakTables(settings.gtfsAgencyId);
        });
    };*/

    const renameTables = async oldSetExists => {
        return dropBakTables(
            settings.gtfsAgencyId
        ).then(() => {
            return renameTempToCurrent(settings.gtfsAgencyId, oldSetExists);
        }).then(status => {
            return dropBakTables(settings.gtfsAgencyId);
        });
    };

    const getFullCSVFileName = gtfsModel => { return path.resolve(settings.unzipDir, gtfsModel.name + '.txt'); }

    const processGTFSModel = async gtfsModel => {
        let tableName = getTempTableName(settings.gtfsAgencyId, gtfsModel);
        let tableSpecs = db.modelToTableSpecs(tableName, gtfsModel.model, false);
        allTableSpecs[gtfsModel.name] = tableSpecs;
        addMessage('importing ' + gtfsModel.name);
        return db.checkCreateTable2(
            tableSpecs, resolveForeignKeyTableName
        ).then(() => {
            return readGTFSModelCSV(gtfsModel, tableSpecs);
        }).then((results) => {
            addMessage('imported ' + gtfsModel.name + ' (' + importInfo[gtfsModel.name].addedRecords + ')');
        }).catch(err => {
            console.log('processGTFSModel: ' + tableName + ' ' + err.message);
            throw (err);
        });
    };

    const resolveForeignKeyTableName = (tableName) => {
        let gtfsModel = GTFSModelsMap[tableName];
        return gtfsModel !== undefined ? getTempTableName(settings.gtfsAgencyId, gtfsModel) : tableName;
    };

    const prepareRecord = (gtfsModel, csvLine) => {
        let record;
        let model = gtfsModel.model;
        let hasNonNull = false;

        for (let i in csvLine) { let csvValue = csvLine[i]; if (csvValue != null && csvValue.length == 0) { csvLine[i] = null; } else { hasNonNull = true; } }

        if (hasNonNull) {
            if (preProcessLine[gtfsModel.name]) {
                csvLine = preProcessLine[gtfsModel.name](csvLine);
            }

            if (csvLine) {
                record = [];
                for (let i in model) {
                    let m = model[i];
                    let csvValue = csvLine[i];
                    let hasThisField = csvValue !== undefined && csvValue != null;
                    if (!hasThisField && m.required && !m.autoIncrement) {
                        throw (new Error('record without required field: ' + gtfsModel.name + ' ' + m.name));
                        record = undefined;
                        break;
                    }
                    record.push(csvValue);
                }
                if (record && record.length == 0) { record = undefined; }
            }
        }
        return record;
    };

    const fillCalendarTable = async () => {
        let calendarRecords = records[csvFileNamesMap.calendar];
        let gtfsModel = GTFSModelsMap.calendar;
        let fillRecords = [];;
        for (let i in calendarRecords) {
            let preparedRecord = prepareRecord(gtfsModel, calendarRecords[i]);
            if (preparedRecord) {
                fillRecords.push(preparedRecord);
            }
        }
        importInfo[csvFileNamesMap.calendar].addedRecords = fillRecords.length;
        return addRecords(gtfsModel, allTableSpecs[csvFileNamesMap.calendar], fillRecords);
    };

    const readGTFSModelCSV = async (gtfsModel, tableSpecs) => {
        let discardForeignKeys;
        if (gtfsModel.name == csvFileNamesMap.calendar_dates) {
            hasCalendarFile = importInfo[csvFileNamesMap.calendar].addedRecords > 0;
            discardForeignKeys = !hasCalendarFile;
        }
        if (discardForeignKeys) {
            addMessage('reading calendar dates without calendar table');
            return doReadGTFSModelCSV(
                gtfsModel, tableSpecs
            ).then(() => {
                return Promise.resolve(fillCalendarTable()).reflect();
            }).then(() => {
                addMessage('calendar table created from calendar dates');
                return true;
            });
        }
        else {
            return doReadGTFSModelCSV(
                gtfsModel, tableSpecs
            ).then(result => {
                return true;
            }).catch(err => {
                throw (err);
            });
        }
    };

    const doReadGTFSModelCSV = async (gtfsModel, tableSpecs) => {
        return new Promise((resolve, reject) => {
            const fullFileName = getFullCSVFileName(gtfsModel);
            const cb = (err, result) => {
                if (err) {
                    reject(err);
                }
                else {
                    importInfo[gtfsModel.name].addedRecords = result;
                    resolve(result);
                }
            };
            if (fs.existsSync(fullFileName)) {
                let gtfsCSVStream = GTFSCSVStream({
                    fileName: fullFileName,
                    gtfsModel: gtfsModel,
                    tableSpecs: tableSpecs,
                    chunkSize: 10000,
                    addRecords: addRecordsCB,
                    prepareRecord: prepareRecord,
                    cb: cb
                });
            }
            else if (gtfsModel.required) { reject(new Error(`csv file "${gtfsModel.name}" does not exist`)); }
            else {
                addMessage('optional file ' + gtfsModel.name + ' is not included', true, false);
                importInfo[gtfsModel.name].addedRecords = 0;
                resolve(0);
            }
        }).catch((err) => {
            throw (err);
        });
    };

    const GTFSCSVStream = function(settings) {
        let theThis; if (!((theThis = this) instanceof GTFSCSVStream)) { return new GTFSCSVStream(settings); }
        const prepareRecord = record => { return settings.prepareRecord(settings.gtfsModel, record); };
        const addRecords = (records, cb) => { return settings.addRecords(settings.gtfsModel, settings.tableSpecs, records, cb); };
        const initialize = () => {
            csvio.csvParseWrite({ fileName: settings.fileName, prepareRecord: prepareRecord, addRecords: addRecords, chunkSize: settings.chunkSize },
                (err, results) => { settings.cb(err, results); });
        };
        initialize();
    };

    const addRecordsCB = (gtfsModel, tableSpecs, records, cb) => {
        return db.insertNoDuplicateKey(tableSpecs.tableName, tableSpecs.fieldNames, [records]).then(() => { cb(); }).catch(err => { err.message = gtfsModel.name + ': ' + err.message; cb(err); });
    };

    const addRecords = async (gtfsModel, tableSpecs, records) => {
        return db.insertNoDuplicateKey(
            tableSpecs.tableName, tableSpecs.fieldNames, [records]
        ).catch(err => {
            err.message = gtfsModel.name + ': ' + err.message;
            throw (err);
        });
    };

    const keyExists = (tableName, keyValue) => { return (records[tableName] !== undefined && records[tableName][keyValue] !== undefined) ? records[tableName][keyValue] : undefined; };
    const keyExistsOrIsNull = (tableName, keyValue) => { return keyValue == null || keyExists(tableName, keyValue); };
    const shapeExistsOrIsNull = (shapeId) => { return shapeId == null || shapeObjs[shapeId] !== undefined; };

    const getDefaultCalendarRecordWithServiceId = serviceId => {
        let record = Object.assign({}, defaultCalendarRecord);
        record[calendarFieldNamesMap.service_id] = serviceId;
        return record;
    };

    const addMessage = (msg, isWarning, isError) => {
        console.log(msg);
        let notifyObj = { importId: importId, order: ++notifyOrder, message: msg };
        if (isWarning) { notifyObj.isWarning = true; }
        else if (isError) { notifyObj.isError = true; }
        if (settings.userObj) { notifyObj.user = settings.userObj; }
        getAgency().notifyAgencyProgress(settings.gtfsAgencyId, notifyObj);
        importInfo.messages.push(msg);
    };

    const createDefaultCalendarRecord = () => {
        let dcr = {};
        dcr[calendarFieldNamesMap.service_id] = 0;
        dcr[calendarFieldNamesMap.monday] = 0;
        dcr[calendarFieldNamesMap.tuesday] = 0;
        dcr[calendarFieldNamesMap.wednesday] = 0;
        dcr[calendarFieldNamesMap.thursday] = 0;
        dcr[calendarFieldNamesMap.friday] = 0;
        dcr[calendarFieldNamesMap.saturday] = 0;
        dcr[calendarFieldNamesMap.sunday] = 0;
        dcr[calendarFieldNamesMap.wd_mask] = 0;
        dcr[calendarFieldNamesMap.wd_mask_name] = gtfsByDateWDMaskStr;
        dcr[calendarFieldNamesMap.start_date] = null;
        dcr[calendarFieldNamesMap.end_date] = null;
        dcr[calendarFieldNamesMap.start_date_date] = null;
        dcr[calendarFieldNamesMap.end_date_date] = null;
        return dcr;
    };

    const addRecord = (table, key, csvLine, addCSVLine) => {
        let resultCSVLine;
        if (key !== undefined) {
            if (records[table] !== undefined) {
                if (records[table][key] === undefined) {
                    let nextCounter = ++autoIncIdCountersTable[table];
                    agencyIdsByAutoIncIds[table]['' + nextCounter] = key;
                    csvLine[autoIncKeyColumnName] = nextCounter;
                    records[table][key] = addCSVLine ? csvLine : { id: nextCounter };
                    resultCSVLine = csvLine;
                }
                else { throw (new Error(table + ' record with duplicate key')); }
            }
        }
        else { throw (new Error(table + ' record with missing key')); }
        return resultCSVLine;
    };

    const processAgencies = csvLine => {
        let table = csvFileNamesMap.agency;
        let key = csvLine[routesFieldNamesMap.agency_id];
        if (!key) {
            key = csvLine[routesFieldNamesMap.agency_id] = '' + (autoIncIdCountersTable[table] + 1);
        }
        return addRecord(table, key, csvLine);
    }

    const processRoutes = csvLine => {
        let table = csvFileNamesMap.routes;
        let key = csvLine[routesFieldNamesMap.route_id];
        let agencyId = csvLine[routesFieldNamesMap.agency_id];
        if (!agencyId) {
            agencyId = agencyIdsByAutoIncIds[csvFileNamesMap.agency][1];
        }
        let agencyObj = records[csvFileNamesMap.agency][agencyId];
        if (agencyObj) {
            csvLine[routesFieldNamesMap.agency_id_in_agency] = agencyIdsByAutoIncIds[csvFileNamesMap.agency][agencyObj.id];
            csvLine[routesFieldNamesMap.agency_id] = agencyObj.id;
        }
        else { throw (new Error(table + ' record with invalid agency ' + agencyId)); }
        let routeType = csvLine[routesFieldNamesMap.route_type];
        if (routeType === undefined || routeType === null) { routeType = csvLine[routesFieldNamesMap.route_type] = gtfsDefaultRouteTypeIndex; }
        if (!(routeType >= 0 && routeType < gtfsRouteTypesArray.length)) { throw (new Error(table + ' record id ' + key + ' with invalid type ' + routeType)); }
        let routeShortName = csvLine[routesFieldNamesArray.route_short_name];
        let routeLongName = csvLine[routesFieldNamesArray.route_long_name];
        if (!routeLongName || !routeLongName.length) { if (routeShortName && routeShortName.length) { csvLine[routesFieldNamesArray.route_long_name] = routeShortName; } }
        let retCSVLine = addRecord(table, key, csvLine);
        records[table][key].routeType = routeType;
        return retCSVLine;
    };

    const processStops = csvLine => {
        let table = csvFileNamesMap.stops;
        let key = csvLine[stopsFieldNamesMap.stop_id];
        let lon = csvLine[stopsFieldNamesMap.stop_lon];
        let lat = csvLine[stopsFieldNamesMap.stop_lat];
        let locationType = csvLine[stopsFieldNamesMap.locationType];
        if (!locationType === null || locationType === undefined) { csvLine[stopsFieldNamesMap.location_type] = locationType = gtfsStopDefaultLocationType; }
        if (!(locationType >= 0 && locationType < gtfsStopLocationTypesArray.length)) { throw (new Error(table + ' record id ' + key + ' with invalid location type ' + locationType)); }
        let isStation = locationType === gtfsStopLocationTypesMap.Station.value;
        let parentStation = csvLine[stopsFieldNamesMap.parent_station];
        let isInStation = false;
        if (parentStation !== null && parentStation !== undefined) {
            if (isStation) { throw (new Error(table + ' record id ' + key + ' stations cannot contain other stations')); }
            else { isInStation = true; }
        }
        let wheelChairBoarding = csvLine[stopsFieldNamesMap.wheelchair_boarding];
        if (wheelChairBoarding === null || wheelChairBoarding === undefined) { csvLine[stopsFieldNamesMap.wheelchair_boarding] = wheelChairBoarding = gtfsStopDefautWheelChairBoarding; }
        if (!(wheelChairBoarding >= 0 && wheelChairBoarding < gtfsStopWheelChairBordingArray.length)) {
            throw (new Error(table + ' record id ' + key + ' with invalid wheelchair boarding type ' + wheelChairBoarding));
        }
        csvLine[stopsFieldNamesMap.stop_point] = db.getPointFrom(lon, lat);
        let retCSVLine = addRecord(table, key, csvLine);
        addToExtent(records[table][key].stopPoint = [+lon, +lat]);
        records[table][key].isStation = isStation;
        if (isInStation) { if (!inStationObjs[key]) { ++nInStationObjs; inStationObjs[key] = records[table][key]; } }
        return retCSVLine;
    };

    const processShapes = csvLine => {
        let table = csvFileNamesMap.shapes;
        let shape_id = csvLine[shapesFieldNamesMap.shape_id];
        let sequence = csvLine[shapesFieldNamesMap.shape_pt_sequence];
        if (!!shape_id && !!sequence && (+sequence) == parseInt(sequence, 10)) {
            let shapeIdObj = shapeObjs[shape_id];
            let shapeId;
            if (shapeIdObj === undefined) { shapeId = ++shapeIdCounter; shapeObjs[shape_id] = { shape_id: shape_id, shapeId: shapeId }; shapeIdsByAutoIncKey['' + shapeId] = shape_id; }
            else { shapeId = shapeIdObj.shapeId; }
            let key = shape_id + '|' + sequence;
            csvLine[shapesFieldNamesMap.shape_id] = shapeId;
            return addRecord(table, key, csvLine);
        }
        else { throw (new Error(table + ' record with missing key')); }
    };

    const processCalendar = csvLine => {
        if (hasCalendarFile) {
            let table = csvFileNamesMap.calendar;
            let key = csvLine[calendarFieldNamesMap.service_id];
            csvLine[calendarFieldNamesMap.wd_mask_name] = gtfsGetFriendlyDayMaskName(csvLine[calendarFieldNamesMap.wd_mask] = gtfsGetDayMaskFromCalendarObj(csvLine));
            csvLine[calendarFieldNamesMap.start_date_date] = csvLine[calendarFieldNamesMap.start_date];
            csvLine[calendarFieldNamesMap.end_date_date] = csvLine[calendarFieldNamesMap.end_date];
            return addRecord(table, key, csvLine);
        }
        else { return csvLine; }
    };

    const processCalendarDates = csvLine => {
        let table = csvFileNamesMap.calendar_dates;
        let service_id = csvLine[calendar_datesFieldNamesMap.service_id];
        let date = csvLine[calendar_datesFieldNamesMap.date];
        if (!!service_id && !!date) {
            let exception_type = csvLine[calendar_datesFieldNamesMap.exception_type];

            if (exception_type === null || exception_type === undefined) { csvLine[calendar_datesFieldNamesMap.exception_type] = exception_type = gtfsCalendarDatesExceptionTypeDefault; }
            if (!(exception_type > 0 && exception_type <= gtfsCalendarDatesExceptionTypesArray.length)) {
                throw (new Error(table + ' record with invalid exception type id'));
            }

            let key = service_id + '|' + date;
            csvLine = addRecord(table, key, csvLine);
            if (csvLine && !hasCalendarFile) {
                if (!keyExists(csvFileNamesMap.calendar, service_id)) {
                    addRecord(csvFileNamesMap.calendar, service_id, getDefaultCalendarRecordWithServiceId(service_id), true);
                }
            }
            let serviceId = keyExists(csvFileNamesMap.calendar, service_id);
            if (!!serviceId) {
                csvLine[calendar_datesFieldNamesMap.service_id_in_agency] = service_id;
                csvLine[calendar_datesFieldNamesMap.service_id] = serviceId.id;
            }
            else { throw (new Error(table + ' record with invalid service id')); }
            csvLine[calendar_datesFieldNamesMap.date_date] = date;
        }
        else { csvLine = {}; }
        return csvLine;
    };

    const processTrips = csvLine => {
        let table = csvFileNamesMap.trips;
        let key = csvLine[tripsFieldNamesMap.trip_id];
        if (!!key) {
            let service_id = csvLine[tripsFieldNamesMap.service_id];
            let route_id = csvLine[tripsFieldNamesMap.route_id];
            let shape_id = csvLine[tripsFieldNamesMap.shape_id];
            let serviceExists = keyExists(csvFileNamesMap.calendar, service_id);
            let routeExists = keyExists(csvFileNamesMap.routes, route_id);
            let shapeIdObj = shapeObjs[shape_id];
            let hasError = false;
            let errorMsg = table + ' with key ' + key;
            if (!(serviceExists && routeExists && !!shapeIdObj)) {
                hasError = true;
                errorMsg += ' has errors,';
                if (!serviceExists) { errorMsg += ' service does not exist'; }
                if (!routeExists) { errorMsg += ' route does not exist'; }
                if (!shapeIdObj) { errorMsg += ' shape does not exist'; }
            }
            else {
                let directionId = csvLine[tripsFieldNamesMap.direction_id];
                if (directionId != 0 && directionId != 1) {
                    hasError = true;
                    errorMsg += ' has invalid direction id: ' + directionId;
                }
            }

            if (!hasError) {
                let wheelChairAccessible = csvLine[tripsFieldNamesMap.wheelchair_accessible];
                if (wheelChairAccessible === null || wheelChairAccessible === undefined) { csvLine[tripsFieldNamesMap.wheelchair_accessible] = wheelChairAccessible = gtfsTripWheelChairAccesibleDefault; }
                if (!(wheelChairAccessible >= 0 && wheelChairAccessible < gtfsTripWheelChairAccessibleArray.length)) {
                    hasError = true;
                    errorMsg += ' has invalid wheelchair accessibility type: ' + wheelChairAccessible;
                }
            }

            if (!hasError) {
                let bikesAllowed = csvLine[tripsFieldNamesMap.bikes_allowed];
                if (bikesAllowed === null || bikesAllowed === undefined) { csvLine[tripsFieldNamesMap.bikes_allowed] = bikesAllowed = gtfsTripBikesAllowedDefault; }
                if (!(bikesAllowed >= 0 && bikesAllowed < gtfsTripBikesAllowedArray.length)) {
                    hasError = true;
                    errorMsg += ' has invalid bicycle accomodation type: ' + bikesAllowed;
                }
            }

            if (hasError) { throw (new Error(errorMsg)); }
            else {
                csvLine[tripsFieldNamesMap.service_id] = serviceExists.id;
                csvLine[tripsFieldNamesMap.shape_id] = shapeIdObj.shapeId;
                csvLine[tripsFieldNamesMap.route_id] = routeExists.id;
                csvLine[tripsFieldNamesMap.route_type] = routeExists.routeType;
            }

            return addRecord(table, key, csvLine);
        }
        return {};
    };

    const processStopTimes = csvLine => {
        let table = csvFileNamesMap.stop_times;
        let trip_id = csvLine[stop_timesFieldNamesMap.trip_id];
        let stop_id = csvLine[stop_timesFieldNamesMap.stop_id];
        let sequence = csvLine[stop_timesFieldNamesMap.stop_sequence];
        let key = (!!trip_id && !!stop_id && !!sequence) ? trip_id + '|' + stop_id + '|' + sequence : undefined;
        if (!!key) {
            let tripExists = keyExists(csvFileNamesMap.trips, trip_id);
            let stopExists = keyExists(csvFileNamesMap.stops, stop_id);
            if (!(tripExists && stopExists)) {
                let msg = table + ' with key ' + key + ' has errors,';
                if (!tripExists) { msg += ' trip does not exist'; }
                if (!stopExists) { msg += ' stop does not exist'; }
                throw (new Error(msg));
            }

            let arrivalTime = csvLine[stop_timesFieldNamesMap.arrival_time];
            let departureTime = csvLine[stop_timesFieldNamesMap.departure_time];
            let timesErrorMsg = table + ' with key ' + key + ' has errors, arrival and/or departure times are invalid';
            let hasTimesError = false;

            if (!departureTime) {
                if (arrivalTime) {
                    csvLine[stop_timesFieldNamesMap.departure_time] = arrivalTime;
                    arrivalTime = departureTime = geom.getHMS(arrivalTime);
                }
                else { hasTimesError = true; }
            }
            else if (!arrivalTime) {
                csvLine[stop_timesFieldNamesMap.arrival_time] = arrivalTime = departureTime;
                arrivalTime = departureTime = geom.getHMS(departureTime);
            }
            else {
                arrivalTime = geom.getHMS(arrivalTime);
                departureTime = geom.getHMS(departureTime);
            }

            if (!hasTimesError) { hasTimesError = (!(arrivalTime.valid && departureTime.valid)) || (arrivalTime.hms > departureTime.hms); }

            if (hasTimesError) { throw (new Error(timesErrorMsg)); return undefined; }

            let pickUpType = csvLine[stop_timesFieldNamesMap.pickup_type];
            if (pickUpType === null || pickUpType === undefined) { csvLine[stop_timesFieldNamesMap.pickup_type] = pickUpType = gtfsStopDefautPickupDropOffType; }
            if (!(pickUpType >= 0 && pickUpType < gtfsPickupDropTypesArray.length)) { throw (new Error(table + ' record with invalid pick up type ' + pickUpType)); }

            let dropOffType = csvLine[stop_timesFieldNamesMap.drop_off_type];
            if (dropOffType === null || dropOffType === undefined) { csvLine[stop_timesFieldNamesMap.drop_off_type] = dropOffType = gtfsStopDefautPickupDropOffType; }
            if (!(dropOffType >= 0 && dropOffType < gtfsPickupDropTypesArray.length)) { throw (new Error(table + ' record with invalid drop off type ' + dropOffType)); }

            let timePoint = csvLine[stop_timesFieldNamesMap.timepoint];
            if (timePoint === null || timePoint === undefined) { csvLine[stop_timesFieldNamesMap.timepoint] = timePoint = gtfsDefaultStopTimeTimePointType; }
            if ((!timePoint >= 0 && timePoint < gtfsStopTimeTimePointTypesArray)) { throw (new Error(table + ' record with invalid timepoint type ' + timePoint)); }

            csvLine[stop_timesFieldNamesMap.arrival_hms] = arrivalTime.hms;
            csvLine[stop_timesFieldNamesMap.departure_hms] = departureTime.hms;

            csvLine[stop_timesFieldNamesMap.trip_id] = tripExists.id;
            csvLine[stop_timesFieldNamesMap.stop_id] = stopExists.id;
        }
        return csvLine;
    };

    const getShapePoints = async shapeId => {
        let shapesGTFSModel = GTFSModelsMap.shapes;
        let shapesTableName = getTempTableName(settings.gtfsAgencyId, shapesGTFSModel);
        let sqlStr = 'SELECT * FROM ?? WHERE ??.?? = ? ORDER BY ??.??;';
        let values = [shapesTableName, shapesTableName, shapesFieldNamesMap.shape_id, shapeId, shapesTableName, shapesFieldNamesMap.shape_pt_sequence];
        return db.queryPoolValues(sqlStr, values).then(results => { return results; }).catch(err => { console.log(err.message); return []; });
    };

    const importShapeIdToCShapes = async (cShapesModel, tableName, shapeId) => {
        return getShapePoints(
            shapeId
        ).then(shapePoints => {
            let shapeCoords = [], shapeDists = [];
            let hasDists = false;
            let shapeIdInAgency = shapeIdsByAutoIncKey['' + shapeId];
            let shapeObj = shapeObjs[shapeIdInAgency];

            shapePoints.map(shapePoint => {
                let shapeDist = shapePoint[shapesFieldNamesMap.shape_dist_traveled];
                let shapePointCoords = [+shapePoint[shapesFieldNamesMap.shape_pt_lon], +shapePoint[shapesFieldNamesMap.shape_pt_lat]];
                shapeCoords.push(shapePointCoords);
                addToExtent(shapePointCoords);
                if (!!shapeDist) { hasDists = true; } else { shapeDist = 0; }
                shapeDists.push(shapeDist);
            });

            let calcDists = geom.calcLSDistances(shapeCoords);

            if (hasDists) {
                let hasError = false;
                let nDists = shapePoints.length;
                let distanceRatio = 1;

                if (globalDistanceRatio !== undefined) {
                    distanceRatio = globalDistanceRatio;
                }
                else {
                    if (nDists > 1 && calcDists[1] > 0) {
                        distanceRatio = shapeDists[1] / calcDists[1];
                        globalDistanceRatio = distanceRatio;
                    }
                }

                let prevDif = 0;
                for (let i = 0; i < nDists; ++i) {
                    let diff = calcDists[i] * distanceRatio - shapeDists[i], absDiff = Math.abs(diff);
                    if (absDiff - prevDif > 2) { hasError = true; break; }
                    prevDif = absDiff;
                }
                if (hasError) { addMessage(csvFileNamesMap.shapes + ' id ' + shapeIdInAgency + ': provided distances differ from calculated distances', true, false); }
            }

            let lsSimplify = geom.simplifyLS(shapeCoords, lineStringSimplifyTolerance), distSimplify = [];
            for (let i in lsSimplify.indices) { distSimplify.push(calcDists[lsSimplify.indices[i]]); }

            let lsEncoded = polyCode.EncodeLineString(lsSimplify.coords, lineStringPrecision);
            let distEncoded = polyCode.EncodeValues(distSimplify, distancesPrecision);

            let origLSEncoded = polyCode.EncodeLineString(shapeCoords, lineStringPrecision);
            let origDistEncoded = polyCode.EncodeValues(calcDists, distancesPrecision);

            shapeObj.shapeCoords = lsSimplify.coords;
            shapeObj.shapeDists = distSimplify;

            cShapesById[shapeId] = shapeObj;

            let sqlStr = 'INSERT INTO ?? (??.??,??.??,??.??,??.??,??.??,??.??) VALUES (?,?,?,?,?,?);';
            let values = [
                tableName,
                tableName, shapes_compressedFieldNamesMap.id,
                tableName, shapes_compressedFieldNamesMap.shape_id,
                tableName, shapes_compressedFieldNamesMap.shape_points,
                tableName, shapes_compressedFieldNamesMap.shape_dist_traveled,
                tableName, shapes_compressedFieldNamesMap.shape_original_points,
                tableName, shapes_compressedFieldNamesMap.shape_original_dist_traveled,
                shapeId, shapeIdInAgency, lsEncoded, distEncoded, origLSEncoded, origDistEncoded
            ];

            return db.queryPoolValues(sqlStr, values);
        }).then(results => {
            return true;
        }).catch(err => {
            err.message = cShapesModel.name + ': ' + err.message;
            throw (err);
        });
    };

    const ImportCShapes = function(settings) {
        let theThis; if (!((theThis = this) instanceof ImportCShapes)) { return new ImportCShapes(settings); }
        let index;

        const importNext = () => {
            if (index < shapeIdCounter) {
                let shapeId = ++index;
                importShapeIdToCShapes(
                    settings.modelToCreate, settings.tableName, shapeId
                ).then(result => {
                    return importNext();
                }).catch(err => {
                    settings.cb(err);
                });
            }
            else {
                settings.cb();
            }
        };

        const initialize = () => {
            index = 0;
            importNext();
        };

        initialize();
    };

    const importShapesToCShapes = async (cShapesModel, tableName) => {
        return new Promise((resolve, reject) => {
            let importCShapes = ImportCShapes({
                modelToCreate: cShapesModel, tableName: tableName,
                cb: (err, result) => {
                    if (err) { reject(err); }
                    else { resolve(result); }
                }
            });
        }).then(() => { return importInfo[cShapesModel.name].addedRecords = shapeIdCounter; });
    };

    const createCShapes = async () => {
        let cShapesModel = GTFSCustomModelsMap.shapes_compressed;
        let tableName = getTempTableName(settings.gtfsAgencyId, cShapesModel);
        let tableSpecs = db.modelToTableSpecs(tableName, cShapesModel.model, false);
        allTableSpecs[cShapesModel.name] = tableSpecs;
        addMessage('creating ' + cShapesModel.name);
        return db.checkCreateTable2(
            tableSpecs, resolveForeignKeyTableName
        ).then(() => {
            return importShapesToCShapes(cShapesModel, tableName);
        }).then(() => {
            addMessage('created ' + cShapesModel.name + ' (' + importInfo[cShapesModel.name].addedRecords + ')');
        }).catch(err => {
            console.log('createCShapes: ' + tableName + ' ' + err.message);
            throw (err);
        });
    };

    const importTripStopTimes = async tripId => {
        let gtfsModel = GTFSModelsMap.stop_times;
        let tableName = getTempTableName(settings.gtfsAgencyId, gtfsModel);
        let sqlStr = 'SELECT * FROM ?? WHERE ??.?? = ? ORDER BY ??.??;';
        let values = [tableName, tableName, stop_timesFieldNamesMap.trip_id, tripId, tableName, stop_timesFieldNamesMap.stop_sequence];
        return db.queryPoolValues(sqlStr, values).then(results => { return results; }).catch(err => { console.log(err.message); return []; });
    };

    const selectOneByKey = async (tableName, keyName, keyValue) => {
        let sqlStr = 'SELECT * FROM ?? WHERE ??.?? = ? LIMIT 1;';
        let values = [tableName, tableName, keyName, keyValue];
        return db.queryPoolValues(sqlStr, values).then(results => { return results.length > 0 ? results[0] : undefined; }).catch(err => { console.log(err.message); return undefined; });
    };

    const getTrip = async tripId => {
        let gtfsModel = GTFSModelsMap.trips;
        let tableName = getTempTableName(settings.gtfsAgencyId, gtfsModel);
        return selectOneByKey(tableName, autoIncKeyColumnName, tripId);
    };

    const addLongestStopSequenceToRoute = (routeObj, directionId, sseqId, gtfsDirection, stopCount) => {
        if (!!routeObj) {
            if (routeObj.dirSseqs === undefined) { routeObj.dirSseqs = {}; }
            let existingMax = routeObj.dirSseqs['' + directionId];
            if (existingMax === undefined) {
                routeObj.dirSseqs['' + directionId] = { sseqId: sseqId, stopCount: stopCount, gtfsDirection: gtfsDirection };
            }
            else {
                if (existingMax.stopCount < stopCount) {
                    existingMax.stopCount = stopCount;
                    existingMax.sseqId = sseqId;
                    existingMax.gtfsDirection = gtfsDirection;
                }
            }
        }
    };

    const getGTFSDirection = lsCoords => {
        let direction = gtfsDirectionNamesIdMap.clockwise;
        let isClockwise = geom.isLSClockwise(lsCoords);
        if (lsCoords && lsCoords.length > 1) {
            let first = lsCoords[0], last = lsCoords[lsCoords.length - 1];
            let deltaLon = last[0] - first[0], deltaLat = last[1] - first[1];
            if (deltaLon == 0 && deltaLat == 0) { direction = isClockwise ? gtfsDirectionNamesIdMap.clockwise : gtfsDirectionNamesIdMap.cntrclockwise; }
            else {
                if (Math.abs(deltaLon) > Math.abs(deltaLat * 2)) { direction = deltaLon < 0 ? gtfsDirectionNamesIdMap.eastbound : gtfsDirectionNamesIdMap.westbound; }
                else { direction = deltaLat < 0 ? gtfsDirectionNamesIdMap.southbound : gtfsDirectionNamesIdMap.northbound; }
            }
        }
        return direction;
    };

    const addNewStopSequenceObject = async (tripObj, stopCount, stopIdsEncoded, stopsCoords, stops_sseqsTableFiller, stops_sseqs_records) => {
        let gtfsModel = GTFSCustomModelsMap.stop_sequences;
        let tableName = getTempTableName(settings.gtfsAgencyId, gtfsModel);
        let tripRouteId = tripObj[tripsFieldNamesMap.route_id];
        let tripDirectionId = tripObj[tripsFieldNamesMap.direction_id];
        let stopSeqDirection = getGTFSDirection(stopsCoords);
        let tripRouteIdInAgency = agencyIdsByAutoIncIds[csvFileNamesMap.routes][tripRouteId];
        let routeObj = keyExists(csvFileNamesMap.routes, tripRouteIdInAgency);

        let sql = "INSERT INTO ?? (??,??,??,??,??,??,??) VALUES(?,?,?,?,?,?,?);";

        let values = [
            tableName,

            stop_sequencesFieldNamesMap.route_id,
            stop_sequencesFieldNamesMap.route_id_in_agency,
            stop_sequencesFieldNamesMap.direction_id,
            stop_sequencesFieldNamesMap.gtfs_direction,
            stop_sequencesFieldNamesMap.stop_count,
            stop_sequencesFieldNamesMap.stop_ids,
            stop_sequencesFieldNamesMap.trip_headsign,

            tripRouteId,
            tripRouteIdInAgency,
            tripDirectionId,
            stopSeqDirection,
            stopCount,
            stopIdsEncoded,
            tripObj[tripsFieldNamesMap.trip_headsign]
        ];

        return db.queryPoolValues(
            sql, values
        ).then(result => {
            let id = result.insertId;
            addLongestStopSequenceToRoute(routeObj, tripDirectionId, id, stopSeqDirection, stopCount);
            for (let i in stops_sseqs_records) { stops_sseqs_records[i][1] = id; }
            return stops_sseqsTableFiller.AddRecords(
                stops_sseqs_records
            ).then(result => {
                return { id: id };
            });
        }).catch(err => {
            err.message = gtfsModel.name + ': ' + err.message;
            throw (err);
        });
    };

    const getStopSequenceObject = async (tripObj, nStops, stopIdsEncoded, stopsCoords, stops_sseqsTableFiller, stops_sseqs_records) => {
        return new Promise((resolve, reject) => {
            let sskey = stopIdsEncoded + '|' + tripObj[tripsFieldNamesMap.trip_headsign];
            let existingSSK = stopSequenceKeys[sskey];

            if (existingSSK === undefined) {
                return addNewStopSequenceObject(
                    tripObj, nStops, stopIdsEncoded, stopsCoords, stops_sseqsTableFiller, stops_sseqs_records
                ).then(result => {
                    let sseq_id = result.id;
                    let createdSSK = stopSequenceKeys[sskey] = { id: sseq_id, tripObj: tripObj };
                    ++nStopSequenceKeys;
                    resolve(createdSSK);
                }).catch(err => {
                    reject(err);
                });
            }
            else {
                resolve(existingSSK);
            }
        });
    };

    const getStopDistancesObject = async (stop_distancesTableFiller, nStops, distEncoded) => {
        return new Promise((resolve, reject) => {
            let sskey = distEncoded, existingSSK = stopDistanceKeys[sskey];
            if (existingSSK === undefined) {
                return stop_distancesTableFiller.AddRecord(
                    [nStopDistanceKeys + 1, nStops, distEncoded]
                ).then(result => {
                    let createdSSK = stopDistanceKeys[sskey] = { id: result };
                    ++nStopDistanceKeys;
                    resolve(createdSSK);
                }).catch(err => { reject(err); });
            }
            else { resolve(existingSSK); }
        });
    };

    const getStopTimeOffsetsObject = async (stop_hms_offsetsTableFiller, nStops, arrivalOffsetsEncoded, departureOffsetsEncoded, pickUpTypesEncoded, dropOffTypesEncoded, stopTimePointsEncoded, stopHeadsignsJSON) => {
        return new Promise((resolve, reject) => {
            let sskey = arrivalOffsetsEncoded + '|' + departureOffsetsEncoded + '|' + pickUpTypesEncoded + '|' + dropOffTypesEncoded + '|' + stopTimePointsEncoded + '|' + stopHeadsignsJSON;
            let existingSSK = stopTimeOffsetsKeys[sskey];
            if (existingSSK === undefined) {
                return stop_hms_offsetsTableFiller.AddRecord(
                    [nStopTimeOffsetsKeys + 1, nStops, arrivalOffsetsEncoded, departureOffsetsEncoded, pickUpTypesEncoded, dropOffTypesEncoded, stopTimePointsEncoded, stopHeadsignsJSON]
                ).then(result => {
                    let createdSSK = stopTimeOffsetsKeys[sskey] = { id: result };
                    ++nStopTimeOffsetsKeys;
                    resolve(createdSSK);
                }).catch(err => { reject(err); });
            }
            else { resolve(existingSSK); }
        });
    };

    const getStopSequenceObjects = async (
        stop_distancesTableFiller, stop_hms_offsetsTableFiller, tripObj, nStops, stopIdsEncoded,
        distEncoded, stopsCoords, arrivalOffsetsEncoded, departureOffsetsEncoded, pickUpTypesEncoded, dropOffTypesEncoded, stopTimePointsEncoded, stopHeadsignsJSON,
        stops_sseqsTableFiller, stops_sseqs_records) => {

        let sseqObj, sdistObj, sotimesObj;
        return getStopSequenceObject(
            tripObj, nStops, stopIdsEncoded, stopsCoords, stops_sseqsTableFiller, stops_sseqs_records
        ).then(result => {
            sseqObj = result;
            return getStopDistancesObject(stop_distancesTableFiller, nStops, distEncoded);
        }).then(result => {
            sdistObj = result;
            return getStopTimeOffsetsObject(stop_hms_offsetsTableFiller, nStops, arrivalOffsetsEncoded, departureOffsetsEncoded, pickUpTypesEncoded, dropOffTypesEncoded, stopTimePointsEncoded, stopHeadsignsJSON);
        }).then(result => {
            sotimesObj = result;
            return undefined;
        }).then(() => {
            return { sseqObj: sseqObj, sdistObj: sdistObj, sotimesObj: sotimesObj };
        });
    };

    const addStopSequenceToRoute = async (routes_sseqsTableFiller, routeObj, directionId, sseqId) => {
        if (!!routeObj) {
            if (routeObj.sseqs === undefined) { routeObj.sseqs = {}; }
            let existingDir = routeObj.sseqs['' + directionId];
            if (existingDir === undefined) { existingDir = routeObj.sseqs['' + directionId] = {}; }
            let key = '' + sseqId;
            let existingSseq = existingDir[key];
            if (existingSseq === undefined) { existingDir[key] = true; return routes_sseqsTableFiller.AddRecord([routeObj.id, sseqId, directionId]); }
        }
        return true;
    };

    const addShapeToRoute = async (routes_shapesTableFiller, routeObj, directionId, shapeId) => {
        if (!!routeObj) {
            if (routeObj.shapes === undefined) { routeObj.shapes = {}; }
            let existingDir = routeObj.shapes['' + directionId];
            if (existingDir === undefined) { existingDir = routeObj.shapes['' + directionId] = {}; }
            let key = '' + shapeId;
            let existingItem = existingDir[key];
            if (existingItem === undefined) { existingDir[key] = true; return routes_shapesTableFiller.AddRecord([routeObj.id, shapeId, directionId]); }
        }
        return true;
    };

    const importStopSequence = async (stop_distancesTableFiller, stop_hms_offsetsTableFiller, trips_sseqsTableFiller, stops_sseqsTableFiller,
        routes_sseqsTableFiller, routes_shapesTableFiller,
        modelToCreate, tableName, tripId) => {
        return importTripStopTimes(
            tripId
        ).then(stopTimes => {
            let len = stopTimes ? stopTimes.length : 0;
            if (len == 0) {
                //throw (new Error('stop times not found for trip ' + tripId));
                addMessage('stop times not found for trip ' + tripId, true, false);
                return true;
            }
            return getTrip(
                tripId
            ).then(tripObj => {
                if (tripObj) {
                    let distanceRatio = globalDistanceRatio != undefined ? globalDistanceRatio : 1;
                    let nStops = stopTimes ? stopTimes.length : 0;
                    let stopIds = nStops > 0 ? new Array(nStops) : [];
                    let stopDistances = nStops > 0 ? new Array(nStops) : [];
                    let stops_sseqs_records = nStops > 0 ? new Array(nStops) : [];
                    let stopsCoords = nStops > 0 ? new Array(nStops) : [];
                    let arrivalOffsetsHMS = nStops > 0 ? new Array(nStops) : [];
                    let departureOffsetsHMS = nStops > 0 ? new Array(nStops) : [];
                    let pickUpTypes = nStops ? new Array(nStops) : [];
                    let dropOffTypes = nStops ? new Array(nStops) : [];
                    let stopTimePoints = nStops ? new Array(nStops) : [];
                    let stopHeadsigns = nStops ? new Array(nStops) : [];
                    let firstArrival = nStops ? stopTimes[0][stop_timesFieldNamesMap.arrival_hms] : 0;
                    let firstDeparture = nStops ? stopTimes[0][stop_timesFieldNamesMap.departure_hms] : 0;
                    let lastArrival = nStops ? stopTimes[nStops - 1][stop_timesFieldNamesMap.arrival_hms] : 0;
                    let hasDistances = false;

                    stopTimes.map((stopTime, stopIndex) => {
                        let thisStopId = stopTime[stop_timesFieldNamesMap.stop_id];
                        let thisDistance = stopTime[stop_timesFieldNamesMap.shape_dist_traveled];
                        let thisHeadsign = stopTime[stop_timesFieldNamesMap.stop_headsign];
                        if (!thisHeadsign) { thisHeadsign = ''; }
                        stopHeadsigns[stopIndex] = thisHeadsign;
                        stopTimePoints[stopIndex] = stopTime[stop_timesFieldNamesMap.timepoint];
                        if (thisDistance == null) { thisDistance = 0; }
                        stopIds[stopIndex] = thisStopId;
                        stopDistances[stopIndex] = (thisDistance / distanceRatio);
                        hasDistances = hasDistances || thisDistance > 0;
                        stops_sseqs_records[stopIndex] = [thisStopId, 0, stopIndex + 1];
                        arrivalOffsetsHMS[stopIndex] = stopTime[stop_timesFieldNamesMap.arrival_hms] - firstArrival;
                        departureOffsetsHMS[stopIndex] = stopTime[stop_timesFieldNamesMap.departure_hms] - firstDeparture;
                        pickUpTypes[stopIndex] = stopTime[stop_timesFieldNamesMap.pickup_type];
                        dropOffTypes[stopIndex] = stopTime[stop_timesFieldNamesMap.drop_off_type];
                        let stopObj = keyExists(csvFileNamesMap.stops, agencyIdsByAutoIncIds[csvFileNamesMap.stops][thisStopId]);
                        let stopCoord = stopObj.stopPoint;
                        stopsCoords[stopIndex] = stopCoord;
                    });

                    let stopIdsEncoded = polyCode.EncodeValues(stopIds, 0);
                    let distDecoded, distEncoded;
                    let tripShapeId = tripObj[tripsFieldNamesMap.shape_id];

                    if (!!tripShapeId) {
                        let shapeObj = cShapesById[tripShapeId]
                        if (!shapeObj.stopDistances) { shapeObj.stopDistances = {}; }
                        let existingDistances = shapeObj.stopDistances[stopIdsEncoded];
                        if (!existingDistances) {
                            let stopDistances = geom.calcPointDistances(shapeObj.shapeCoords, shapeObj.shapeDists, stopsCoords);
                            if (stopDistances.nFailed > 0) {
                                addMessage(csvFileNamesMap.shapes + ' id ' + shapeObj.shape_id + ' contains ' + stopDistances.nFailed + ' stop sequence errors', true, false);
                            }
                            existingDistances = shapeObj.stopDistances[stopIdsEncoded] = {
                                stopDistances: stopDistances.distances, stopDistancesEncoded: polyCode.EncodeValues(stopDistances.distances, distancesPrecision)
                            };
                        }
                        distDecoded = existingDistances.stopDistances;
                        distEncoded = existingDistances.stopDistancesEncoded;
                    }
                    else {
                        distDecoded = hasDistances ? stopDistances : undefined;
                        distEncoded = hasDistances ? polyCode.EncodeValues(stopDistances, distancesPrecision) : undefined;
                    }

                    let tripDirectionId = tripObj[tripsFieldNamesMap.direction_id];
                    let tripRouteId = tripObj[tripsFieldNamesMap.route_id];
                    let routeObj = keyExists(csvFileNamesMap.routes, agencyIdsByAutoIncIds[csvFileNamesMap.routes][tripRouteId]);

                    let arrivalOffsetsEncoded = polyCode.EncodeValues(arrivalOffsetsHMS, 0);
                    let departureOffsetsEncoded = polyCode.EncodeValues(departureOffsetsHMS, 0);

                    let pickUpTypesEncoded = polyCode.EncodeValues(pickUpTypes, 0);
                    let dropOffTypesEncoded = polyCode.EncodeValues(dropOffTypes, 0);

                    let stopTimePointsEncoded = polyCode.EncodeValues(stopTimePoints, 0);
                    let stopHeadsignsJSON = JSON.stringify(stopHeadsigns);

                    return getStopSequenceObjects(
                        stop_distancesTableFiller, stop_hms_offsetsTableFiller, tripObj, nStops, stopIdsEncoded, distEncoded, stopsCoords,
                        arrivalOffsetsEncoded, departureOffsetsEncoded, pickUpTypesEncoded, dropOffTypesEncoded, stopTimePointsEncoded, stopHeadsignsJSON,
                        stops_sseqsTableFiller, stops_sseqs_records
                    ).then(result => {
                        let ssObj = result.sseqObj, sdistObj = result.sdistObj, sotimesObj = result.sotimesObj;
                        let sseqId = ssObj.id, sdistId = sdistObj.id, sotimesId = sotimesObj.id;

                        return trips_sseqsTableFiller.AddRecord(
                            [tripId, sseqId, sdistId, sotimesId, firstArrival, lastArrival]
                        )

                        .then(results => {
                            return addStopSequenceToRoute(routes_sseqsTableFiller, routeObj, tripDirectionId, sseqId);
                        }).then(results => {
                            return !!tripShapeId ? addShapeToRoute(routes_shapesTableFiller, routeObj, tripDirectionId, tripShapeId) : true;
                        });
                    });
                }
                else {
                    throw (new Error('importStopSequence: failed to retrieve trip ' + tripId));
                }
            });
        });
    };

    const ImportStopSequences = function(importSettings) {
        let theThis; if (!((theThis = this) instanceof ImportStopSequences)) { return new ImportStopSequences(importSettings); }
        let index;

        const importNext = () => {
            if (index < autoIncIdCountersTable[csvFileNamesMap.trips]) {
                let tripId = ++index;
                importStopSequence(
                    importSettings.stop_distancesTableFiller,
                    importSettings.stop_hms_offsetsTableFiller,
                    importSettings.trips_sseqsTableFiller,
                    importSettings.stops_sseqsTableFiller,
                    importSettings.routes_sseqsTableFiller,
                    importSettings.routes_shapesTableFiller,
                    importSettings.modelToCreate, importSettings.tableName, tripId
                ).then(result => { return importNext(); }).catch(err => { importSettings.cb(err); });
            }
            else { importSettings.cb(); }
        };

        const initialize = () => { index = 0; importNext(); };

        initialize();
    };

    const importStopSequences = async (modelToCreate, tableName, stop_distancesTableFiller, stop_hms_offsetsTableFiller, trips_sseqsTableFiller,
        stops_sseqsTableFiller, routes_sseqsTableFiller, routes_shapesTableFiller) => {
        return new Promise((resolve, reject) => {
            ImportStopSequences({
                modelToCreate: modelToCreate, tableName: tableName,
                stop_distancesTableFiller: stop_distancesTableFiller,
                stop_hms_offsetsTableFiller: stop_hms_offsetsTableFiller,
                trips_sseqsTableFiller: trips_sseqsTableFiller,
                stops_sseqsTableFiller: stops_sseqsTableFiller,
                routes_sseqsTableFiller: routes_sseqsTableFiller,
                routes_shapesTableFiller: routes_shapesTableFiller,
                cb: (err, result) => { if (err) { reject(err); } else { resolve(result); } }
            });
        }).then(() => { return importInfo[modelToCreate.name].addedRecords = nStopSequenceKeys; });
    };

    const TableFiller = function(fillerSettings) {
        let theThis; if (!((theThis = this) instanceof TableFiller)) { return new TableFiller(fillerSettings); }
        let tableName, tableSpecs, addedCount, tableRecords;

        this.CreateTable = async () => { return db.checkCreateTable2(tableSpecs, resolveForeignKeyTableName); };

        this.AddRecord = async (record) => {
            tableRecords.push(record);
            return (tableRecords.length >= fillerSettings.chunkSize) ? addRecords() : addedCount + tableRecords.length;
        };

        this.AddRecords = async (records) => {
            tableRecords.push.apply(tableRecords, records);
            return (tableRecords.length >= fillerSettings.chunkSize) ? addRecords() : addedCount + tableRecords.length;
        };

        this.OnEnd = async () => { return tableRecords.length > 0 ? addRecords() : addedCount; };

        const addRecords = async () => {
            let addLen = tableRecords.length;
            if (addLen > 0) {
                //if (fillerSettings.modelToCreate.name == GTFSCustomModelsMap.trips_sseqs.name) { console.log('here'); }
                return db.insertNoDuplicateKey(
                    tableName, tableSpecs.fieldNames, [tableRecords]
                ).then(() => {
                    addedCount += addLen;
                    tableRecords = [];
                    return addedCount;
                }).catch(err => {
                    err.message = fillerSettings.modelToCreate.name + ': ' + err.message;
                    throw (err);
                });
            }
            else { return addedCount; }
        };

        const initialize = () => {
            tableName = getTempTableName(settings.gtfsAgencyId, fillerSettings.modelToCreate);
            tableSpecs = db.modelToTableSpecs(tableName, fillerSettings.modelToCreate.model, false);
            allTableSpecs[fillerSettings.modelToCreate.name] = tableSpecs;
            addedCount = 0;
            tableRecords = [];
        };

        initialize();
    };

    const SetRouteDirections = function(setRouteSettings) {
        let theThis; if (!((theThis = this) instanceof SetRouteDirections)) { return new SetRouteDirections(setRouteSettings); }
        let routesDirectionsTableFiller, routesShapeTableFiller , index;

        const setRouteDirection = async (routeObj, routeId, routeIdInAgency, directionId, routeDirSseqs) => {
            let mlsCoords;
            if (routeDirSseqs) {
                mlsCoords = [];
                if (routeObj.shapes) {
                    let existingDir = routeObj.shapes['' + directionId];
                    if (existingDir) {
                        for (let i in existingDir) { let shapeObj = cShapesById[i]; if (shapeObj) { mlsCoords.push(shapeObj.shapeCoords); } }
                        mlsCoords = geom.MergeMLS({}).Merge(mlsCoords).coordinates;
                        for (let i in mlsCoords) { mlsCoords[i] = polyCode.EncodeLineString(mlsCoords[i], lineStringPrecision); }
                    }
                }
                let mlsCoordsJSON = JSON.stringify(mlsCoords);
                return routesDirectionsTableFiller.AddRecord(
                    [routeId, directionId, routeDirSseqs.gtfsDirection, mlsCoordsJSON]
                ).then(result => { return mlsCoords; });
            }
            else { return mlsCoords; }
            //else { addMessage(routeIdInAgency + ' has no trips in direction ' + directionId); }
        };

        const setRouteDirections = async routeId => {
            let routeRecords = records[csvFileNamesMap.routes];
            let routeIdInAgency = agencyIdsByAutoIncIds[csvFileNamesMap.routes][routeId];
            let routeObj = routeRecords[routeIdInAgency];
            let routeDirSseqs = routeObj.dirSseqs;
            if (routeDirSseqs) {
                let allMlsCoords = [];
                return setRouteDirection(
                    routeObj, routeId, routeIdInAgency, '0', routeDirSseqs['0']
                ).then(mlsCoords => {
                    if (mlsCoords) { allMlsCoords.push.apply(allMlsCoords, mlsCoords); }
                    return setRouteDirection(routeObj, routeId, routeIdInAgency, '1', routeDirSseqs['1'])
                }).then(mlsCoords => {
                    if (mlsCoords) { allMlsCoords.push.apply(allMlsCoords, mlsCoords); }
                    return routesShapeTableFiller.AddRecord(
                        [routeId, JSON.stringify(allMlsCoords)]
                    );
                });
            }
            else {
                addMessage('there are no trips assigned to route ' + routeIdInAgency, true, false);
                return true;
            }
        };

        const setNext = () => {
            if (index < autoIncIdCountersTable[csvFileNamesMap.routes]) {
                setRouteDirections(++index).then(result => { return setNext(); }).catch(err => { setRouteSettings.cb(err); });
            }
            else {
                let routeDirectionsCount = 0, routeShapesCount = 0;
                routesDirectionsTableFiller.OnEnd(
                ).then(result => {
                    routeDirectionsCount = result;
                    return routesShapeTableFiller.OnEnd();
                }).then(result => {
                    routeShapesCount = result;
                    setRouteSettings.cb(undefined, { routeDirectionsCount: routeDirectionsCount, routeShapesCount: routeShapesCount });
                }).catch(err => {
                    setRouteSettings.cb(err);
                });
            }
        };

        const initialize = () => {
            routesDirectionsTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.routes_directions, chunkSize: setRouteSettings.chunkSize });
            routesShapeTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.routes_shape, chunkSize: setRouteSettings.chunkSize });
            routesDirectionsTableFiller.CreateTable(
            ).then(result => {
                return routesShapeTableFiller.CreateTable();
            }).then(result => {
                index = 0; setNext();
            });
        };

        initialize();
    };

    const assignRouteDirections = async () => {
        return new Promise((resolve, reject) => { SetRouteDirections({ cb: (err, result) => { if (err) { reject(err); } else { resolve(result); } } }); });
    };

    const addModelTableIndices = async (model, indices) => {
        let tableName = getTempTableName(settings.gtfsAgencyId, model);
        let sqlStr = "ALTER TABLE ?? ";
        let addIndexStr = "";
        let values = [tableName];
        let nIndices = indices.length;
        for (let i = 0; i < nIndices; ++i) {
            let fieldName = indices[i];
            if (addIndexStr.length) { addIndexStr += ','; }
            addIndexStr += "ADD INDEX(??)";
            values.push(fieldName);
        }
        sqlStr += addIndexStr;
        return db.queryPoolValues(
            sqlStr, values
        ).then(results => {
            return true;
        }).catch(err => {
            console.log('addModelTableIndices(' + model.name  + '): ' + err.message);
            throw (new Error('unexpected error adding indices to ' + model.name + 'table'));
        });
    };

    const addTripsIndices = async () => { return addModelTableIndices(GTFSModelsMap.trips, tripsIndices); };

    const createStopSequences = async () => {
        let modelToCreate = GTFSCustomModelsMap.stop_sequences;
        let tableName = getTempTableName(settings.gtfsAgencyId, modelToCreate);
        let tableSpecs = db.modelToTableSpecs(tableName, modelToCreate.model, false);
        let chunkSize = 1000;
        let stop_distancesTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.stop_distances, chunkSize: chunkSize });
        let stop_hms_offsetsTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.stop_hms_offsets, chunkSize: chunkSize });
        let trips_sseqsTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.trips_sseqs, chunkSize: chunkSize });
        let stops_sseqsTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.stops_sseqs, chunkSize: chunkSize });
        let routes_sseqsTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.routes_sseqs, chunkSize: chunkSize });
        let routes_shapesTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.routes_shapes, chunkSize: chunkSize });

        allTableSpecs[modelToCreate.name] = tableSpecs;

        addMessage('creating ' + modelToCreate.name);
        return db.checkCreateTable2(
            tableSpecs, resolveForeignKeyTableName
        ).then(() => {
            return stop_distancesTableFiller.CreateTable();
        }).then(() => {
            return stop_hms_offsetsTableFiller.CreateTable();
        }).then(() => {
            return trips_sseqsTableFiller.CreateTable();
        }).then(() => {
            return stops_sseqsTableFiller.CreateTable();
        }).then(() => {
            return routes_sseqsTableFiller.CreateTable();
        }).then(() => {
            return routes_shapesTableFiller.CreateTable();
        }).then(() => {
            return importStopSequences(
                modelToCreate, tableName, stop_distancesTableFiller, stop_hms_offsetsTableFiller, trips_sseqsTableFiller,
                stops_sseqsTableFiller, routes_sseqsTableFiller, routes_shapesTableFiller
            );
        }).then(result => {
            return stop_distancesTableFiller.OnEnd();
        }).then(result => {
            importInfo[GTFSCustomModelsMap.stop_distances.name].addedRecords = result;
            return stop_hms_offsetsTableFiller.OnEnd();
        }).then((result) => {
            importInfo[GTFSCustomModelsMap.stop_hms_offsets.name].addedRecords = result;
            return trips_sseqsTableFiller.OnEnd();
        }).then(result => {
            importInfo[GTFSCustomModelsMap.trips_sseqs.name].addedRecords = result;
            return stops_sseqsTableFiller.OnEnd();
        }).then(result => {
            importInfo[GTFSCustomModelsMap.stops_sseqs.name].addedRecords = result;
            return routes_sseqsTableFiller.OnEnd();
        }).then(result => {
            importInfo[GTFSCustomModelsMap.routes_sseqs.name].addedRecords = result;
            return routes_shapesTableFiller.OnEnd();
        }).then(result => {
            importInfo[GTFSCustomModelsMap.routes_shapes.name].addedRecords = result;
            return assignRouteDirections({ chunkSize: chunkSize });
        }).then((result) => {
            importInfo[GTFSCustomModelsMap.routes_directions.name].addedRecords = result.routeDirectionsCount;
            importInfo[GTFSCustomModelsMap.routes_shape.name].addedRecords = result.routeShapesCount;
            addMessage('created ' + modelToCreate.name + ' (' + importInfo[modelToCreate.name].addedRecords + ')');
        }).catch(err => {
            console.log('createStopSequences: ' + tableName + ' ' + err.message);
            throw (err);
        });
    };

    const createCurrentInfo = async () => {
        let current_infoTableFiller = TableFiller({ modelToCreate: GTFSCustomModelsMap.current_info, chunkSize: 10 });
        return current_infoTableFiller.CreateTable(
        ).then(results => {
            let nSubAgencies = importInfo[GTFSModelsMap.agency.name].addedRecords;
            let nServices = importInfo[GTFSModelsMap.calendar.name].addedRecords;
            let nRoutes = importInfo[GTFSModelsMap.routes.name].addedRecords;
            let nStops = importInfo[GTFSModelsMap.stops.name].addedRecords;
            let nTrips = importInfo[GTFSModelsMap.trips.name].addedRecords;
            let nStopSequences = importInfo[GTFSCustomModelsMap.stop_sequences.name].addedRecords;
            let nStopDistances = importInfo[GTFSCustomModelsMap.stop_distances.name].addedRecords;
            let nStopTimes = importInfo[GTFSCustomModelsMap.stop_hms_offsets.name].addedRecords;
            let nShapes = importInfo[GTFSCustomModelsMap.shapes_compressed.name].addedRecords;
            let agencyExtentBlob = JSON.stringify(agencyExtent);
            let publishedDate = db.YYYYMMDDToDate(new Date());

            let infoRecord = [settings.gtfsAgencyId, nSubAgencies, nServices, nRoutes, nStops, nTrips, nStopSequences, nStopDistances, nStopTimes, nShapes, agencyExtentBlob, publishedDate];

            return current_infoTableFiller.AddRecord(infoRecord);
        }).then(results => {
            return current_infoTableFiller.OnEnd();
        }).then(results => {
            importInfo[GTFSCustomModelsMap.current_info.name] = results;
        });
    };

    const updateStations = async () => {
        if (nInStationObjs > 0) {
            let stopStops = nInStationObjs === 1 ? "stop" : "stops";
            addMessage('processing ' + nInStationObjs + ' ' + stopStops + ' located inside stations');
            for (let i in inStationObjs) {
                let stopInStation = inStationObjs[i];
                //check if station "stop" exists and is a station, and update the parent_station field to internal id
            }
            addMessage(nInStationObjs + ' inside stations processed');
        }
        else {
            addMessage('there are no stops located inside stations');
        }
    };


    const doCompareTempAndCurrentAgency = async () => {
        addMessage('looking for changes...');
        return compareTempAndCurrentAgency(
            settings.gtfsAgencyId
        ).then(results => {
            //importInfo.prevIsIdenticalToCurrent = false;
            importInfo.prevIsIdenticalToCurrent = !results;
        });
    };

    const initialize = () => {
        polyCode = geom.PolyCode();
        preProcessLine = {};
        preProcessLine[csvFileNamesMap.agency] = processAgencies;
        preProcessLine[csvFileNamesMap.routes] = processRoutes;
        preProcessLine[csvFileNamesMap.stops] = processStops;
        preProcessLine[csvFileNamesMap.shapes] = processShapes;
        preProcessLine[csvFileNamesMap.calendar] = processCalendar;
        preProcessLine[csvFileNamesMap.calendar_dates] = processCalendarDates;
        preProcessLine[csvFileNamesMap.trips] = processTrips;
        preProcessLine[csvFileNamesMap.stop_times] = processStopTimes;
    };

    initialize();
};

let globalPublishId = 0;

const GTFSPublish = function (settings) {
    let theThis; if (!((theThis = this) instanceof GTFSPublish)) { return new GTFSPublish(settings); }
    let publishId, notifyOrder;
    let success;
    let datePublished, hasPublishedOnPublishedDate;
    let publishErr;
    let publishResult;


    this.Publish = async () => {
        datePublished = new Date();
        //datePublished = new Date(2017, 2, 15);
        success = true;
        let miliStart = Date.now();
        addMessage("publish operation has started");
        return checkAgencyHasCurrentSet(
            settings.gtfsAgencyId
        ).then(setExists => {
            if (setExists) {
                return checkHasPublishedOnDate(
                    settings.gtfsAgencyId, datePublished
                );
            }
            else {
                throw new Error("working GTFS set does not exist");
            }
        }).then(results => {
            hasPublishedOnPublishedDate = results;
            return doPublish();
        }).then(results => {
            publishResult = results;
            return undefined;
        }).catch(err => {
            publishErr = err;
            return undoOperations();
        }).then(() => {
            return dropTempAndBakTables(
                settings.gtfsAgencyId
            );
        }).then(() => {
            return getAgency().getAgencyPublications().Update();
        }).then(() => {
            let millis = Date.now() - miliStart;
            let secs = Math.round(millis / 1000);
            let nSecs = ' (' + secs + 's)';
            let isSuccess = !publishErr;
            let message = publishErr ? 'publish: ' + publishErr.message : "publish complete";
            if (!publishErr) {
                console.log('publish: ' + JSON.stringify(publishResult));
            }
            message += nSecs;
            addMessage(message, false, !!publishErr);
            if (isSuccess) {
                getAgency().notifyAgencyChanged(settings.gtfsAgencyId);
            }
            return message;
        });
    };

    const undoOperations = async () => { return hasPublishedOnPublishedDate ? undefined : dropPublishedTables(settings.gtfsAgencyId, datePublished); };

    const addPublishedRecord = (gtfsAgencyId, onDate) => {
        let tableName = getAgencyPublishedTableName(gtfsAgencyId);
        let tableSpecs = db.modelToTableSpecs(tableName, agency_publishedModel, false);
        return db.checkCreateTable2(
            tableSpecs, undefined
        ).then(results => {
            let sqlStr = "INSERT INTO ?? (??) VALUES(?);";
            let values = [tableName, agency_publishedFieldNamesMap.published_date, db.YYYYMMDDToString(datePublished)];
            return db.queryPoolValues(sqlStr, values);
        });
    };

    const doPublish = async () => {
        return dropTempAndBakTables(
            settings.gtfsAgencyId
        ).then(() => {
            return compareCurrentAndPublishedAgency(
                settings.gtfsAgencyId
            );
        }).then(agenciesDiffer => {
            if (agenciesDiffer) {
                if (hasPublishedOnPublishedDate) {
                    return copyCurrentToTemp(
                        settings.gtfsAgencyId
                    ).then(result => {
                        if (!result) { throw new Error('failed to copy working set'); }
                        return renameTempToPublished(
                            settings.gtfsAgencyId, datePublished
                        );
                    });
                }
                else {
                    return copyCurrentToPublished(
                        settings.gtfsAgencyId, datePublished
                    ).then(result => {
                        if (!result) { throw new Error('failed to copy working set'); }
                        return addPublishedRecord(
                            settings.gtfsAgencyId, datePublished
                        );
                    });
                }
            }
            else {
                throw (new Error('no changes were detected; agency was not published'));
            }
        }).then(() => {
            return "publish complete";
        });
    };

    const addMessage = (msg, isWarning, isError) => {
        console.log(msg);
        let notifyObj = { publishId: publishId, order: ++notifyOrder, message: msg };
        if (isWarning) { notifyObj.isWarning = true; }
        else if (isError) { notifyObj.isError = true; }
        if (settings.userObj) { notifyObj.user = settings.userObj; }
        getAgency().notifyAgencyProgress(settings.gtfsAgencyId, notifyObj);
    };

    const initialize = () => {
        publishId = ++globalPublishId;
        notifyOrder = 0;
    };

    initialize();
};

const doTest = () => {
    let connUsing = db.getSqlConnection();
    let queryResult;
    Promise.using(connUsing, (conn) => {
        conn.beginTransaction(
        ).then(result => {
            return conn.query("select 1+1");
        }).then(result => {
            queryResult = result;
            return conn.commit();
        }).then(result => {
            console.log('processing result: ' + queryResult);
        }).catch(err => {
            console.log('processing error: ' + err.message);
            return conn.rollback();
        }).catch(err => {
            console.log('rollback error: ' + err.message);
        }).then(() => {
            setTimeout(() => {
                doTest();
            }, 500);
        });
    });
};

const doTest2 = () => {
    let tableName = getPublishedTableName(444, GTFSModelsMap.shapes, new Date());
    console.log(tableName);
};

const init = async () => {
    //doTest();
    //doTest2();
};

module.exports = {
    init: init,

    getLatestPublishedAgencyInfos: getLatestPublishedAgencyInfos,
    getAgenciesPublishedDates: getAgenciesPublishedDates,

    getCurrentAgencyInfo: getCurrentAgencyInfo,
    getCurrentAgencyInfos: getCurrentAgencyInfos,

    getCurrentAgencies: getCurrentAgencies,
    getCurrentRoutes: getCurrentRoutes,
    getCurrentStops: getCurrentStops,
    getCurrentShapes: getCurrentShapes,
    getCurrentCalendar: getCurrentCalendar,
    getCurrentCalendarDates: getCurrentCalendarDates,
    getCurrentServices: getCurrentServices,
    getCurrentTrips: getCurrentTrips,
    getCurrentStopSequences: getCurrentStopSequences,

    getPublishedCalendarDates: getPublishedCalendarDates,
    getPublishedCalendar: getPublishedCalendar,
    getPublishedServices: getPublishedServices,
    getPublishedShapes: getPublishedShapes,
    getPublishedRoutes: getPublishedRoutes,
    getPublishedAgencies: getPublishedAgencies,
    getPublishedStops: getPublishedStops,
    getPublishedStopSequences: getPublishedStopSequences,
    getPublishedTrips: getPublishedTrips,

    GTFSImport: GTFSImport,
    GTFSPublish: GTFSPublish,
    dropAllAgencyTables: dropAllAgencyTables
};
