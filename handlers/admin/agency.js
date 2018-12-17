'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        return agency.createAgency(
            req.body.prefix
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    get: async (req, res, next) => {
        return agency.listAgencies(
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    put: async (req, res, next) => {
        return agency.changeAgency(
            req.body.id,
            req.body.prefix
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    delete: async (req, res, next) => {
        return agency.deleteAgency(
            req.body.id
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    }
};
