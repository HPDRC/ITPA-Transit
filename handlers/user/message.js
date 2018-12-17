'use strict';

const dataProvider = require('../../db/user');

module.exports = {
    post: async (req, res, next) => {
        let sessionUser = req.session.user;
        if (sessionUser) {
            if (req.body.id && req.body.message) {
                let user = { id: sessionUser.id, email: sessionUser.email, isAdmin: sessionUser.isAdmin };
                dataProvider.sendMessage(
                    req.body.id, { user: user, message: req.body.message }
                ).then(result => {
                    return res.status(200).json(result);
                }).catch(err => {
                    console.log(err.message);
                    return res.status(200).json({ ok: false, message: "internal error" });
                });
            }
            else { res.sendStatus(204); }
        }
        else { res.sendStatus(204); }
    }
};
