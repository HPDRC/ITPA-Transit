'use strict';

const port = process.env.PORT || 1337;
const swaggerUIWebPath = '/api';
const docsPath = '/swagger';

const http = require('http');
const fileupload = require('express-fileupload');
//const multer = require('multer');
const express = require('express');
const bodyParser = require('body-parser');
const swaggerize = require('swaggerize-express');
const swaggerUI = require('swaggerize-ui');
const path = require('path');
const cors = require('cors');

const notify = require('./notify/notify');
const sessions = require('./sessions/sessions');
const db = require('./db/db');
const user = require('./db/user');
const agency = require('./db/agency');

const dbConnSettings = { host: 'localhost', port: 3306, user: 'transit', password: '', connectionLimit: 20 };
const dbName = 'Transit';

db.init({
    dbName: dbName,
    connSettings: dbConnSettings
}).then(() => {
    return user.init();
}).then(() => {
    return agency.init();
}).then(() => {
    var app = express();
    var server = http.createServer(app);

    //process.env.NODE_ENV = 'production';
    process.env.NODE_ENV = 'development';

    app.locals.title = "Transit API";

    app.use(cors({
        "credentials": true,
        "origin": true,
        "methods": "GET,HEAD,PUT,PATCH,POST,DELETE",
        "preflightContinue": false,
        "optionsSuccessStatus": 200
    }));

    app.options('*', cors());

    sessions.init({ app: app, dbConnSettings: dbConnSettings, database: dbName });
    notify.init({ app: app, server: server, sessions: sessions, session: sessions.getAppSession() });

    app.get('*', (req, res, next) => { res.setHeader('Last-Modified', (new Date()).toUTCString()); next(); });

    sessions.setSessionRoutes(app);

    //app.use(express.static(path.join(__dirname, '/public')));
    //app.use('/static', express.static(path.join(__dirname, 'public')));

    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: false }));

    app.use(fileupload());

    //app.use(multer({ dest: path.resolve('./uploads') }).single('file'));

    app.use('/agency/upload', (req, res, next) => {
        if (req.files && req.files.file && req.files.file.mv) { req.body.file = "file"; }
        next();
    });

    sessions.setPostBodySessionRoutes(app);
    
    app.use(swaggerize({
        api: path.resolve('./config/swagger.json'),
        handlers: path.resolve('./handlers'),
        docspath: docsPath
    }));

    app.use(swaggerUIWebPath, swaggerUI({ docs: docsPath }));

    server.listen(port, function () {
        //console.log(app.locals.title + " server started on port " + port);
    });
}).catch(err => {
    console.log(`server initialization failed: ${err.message} ${err.stack}`);
});
