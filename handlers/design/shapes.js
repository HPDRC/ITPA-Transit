'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getDesignShapes(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.shapeIds,
                req.body.shapeIdsInAgency,
                req.body.routeId,
                req.body.routeIdInAgency,
                req.body.routeDirectionId,
                req.body.includeOriginal,
                req.body.excludeSimplified,
                req.body.decodeData,
                req.body.returnGeoJSON
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

