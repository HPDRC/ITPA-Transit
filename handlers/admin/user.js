'use strict';

var user = require('../../db/user');

module.exports = {
    post: async (req, res, next) => {
        return user.createUser(
            req.body.email,
            req.body.password,
            req.body.isAdmin
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    put: async (req, res, next) => {
        return user.modifyUser(
            req.body.id,
            req.body.email,
            req.body.password,
            req.body.isAdmin
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    get: async (req, res, next) => {
        return user.listAllUsers(
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    delete: async (req, res, next) => {
        return user.deleteUserWithId(
            req.body.id
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    }
};
