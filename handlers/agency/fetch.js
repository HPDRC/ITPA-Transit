'use strict';

const dataProvider = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        let sessionUser = req.session.user;
        if (sessionUser) {
            let user = { id: sessionUser.id, email: sessionUser.email, isAdmin: sessionUser.isAdmin };
            dataProvider.importRemoteFile(
                user, req.body.url, req.body.id
            ).then(result => {
                return res.status(200).json(result);
            }).catch(err => {
                console.log(err.message);
                return res.status(200).json({ ok: false, message: "internal error" });
            });
        }
        else {
            res.sendStatus(204);
        }
    }
};
