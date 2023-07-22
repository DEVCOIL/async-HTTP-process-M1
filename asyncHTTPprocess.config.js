// for pm2

module.exports = {
    apps: [
        {
            name: 'async-http-process',
            script: 'npm',
            args: 'start',
        },
    ],
};