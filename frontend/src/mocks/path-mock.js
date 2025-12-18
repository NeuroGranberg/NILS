export default {
    join: (...args) => args.join('/'),
    resolve: (...args) => args.join('/'),
    basename: (p) => p.split('/').pop(),
    extname: (p) => '.' + p.split('.').pop(),
    dirname: (p) => p.split('/').slice(0, -1).join('/'),
    sep: '/',
};
