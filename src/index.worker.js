import initSqlJs from '@jlongster/sql.js';
import { SQLiteFS } from 'absurd-sql';
import IndexedDBBackend from 'absurd-sql/dist/indexeddb-backend';

async function init() {
  let SQL = await initSqlJs({ locateFile: file => file });
  let sqlFS = new SQLiteFS(SQL.FS, new IndexedDBBackend());
  SQL.register_for_idb(sqlFS);

  SQL.FS.mkdir('/sql');
  SQL.FS.mount(sqlFS, {}, '/sql');

  if (typeof SharedArrayBuffer === 'undefined') {
    let stream = SQL.FS.open(path, 'a+');
    await stream.node.contents.readIfFallback();
    SQL.FS.close(stream);
  }

  let db = new SQL.Database('/sql/Northwind_small.db.sqlite3', { filename: true });
  db.exec(`
    PRAGMA cache_size=${(64 * 1000 * 1000)/4096};
    PRAGMA page_size=4096;
    PRAGMA journal_mode=MEMORY;
  `);
  return db;
}

async function runQueries() {
  let db = await init();

  try {
    db.exec('CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)');
  } catch (e) {}

  db.exec('BEGIN TRANSACTION');
  let stmt = db.prepare('INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)');
  for (let i = 0; i < 5; i++) {
    stmt.run([i, ((Math.random() * 100) | 0).toString()]);
  }
  stmt.free();
  db.exec('COMMIT');

  stmt = db.prepare(`SELECT * FROM Customer ORDER BY Address`);
  let fullStart = Date.now();
  let result = stmt.step();
  while (result) {
    // let start = Date.now();
    let data = stmt.getAsObject();
    // let stop = Date.now();
    // console.log(`[${stop - start}ms] Result: `, data);
    console.log(`Result: `, data);
    result = stmt.step();
  }
  let fullStop = Date.now();
  console.log(`Run Time: [${fullStop - fullStart}ms]`)
  stmt.free();
}

runQueries().then(r => {
  console.log('Finished');
});
