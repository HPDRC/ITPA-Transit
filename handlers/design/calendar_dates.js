'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getDesignCalendarDates(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.serviceIds,
                req.body.serviceIdsInAgency,
                req.body.onDate
            ).then((result) => {
                res.status(200).json(result);
            }).catch((err) => {
                res.status(400).send(err.message);
            });
        }
        else {
            res.sendStatus(204);
        }
    }
};

