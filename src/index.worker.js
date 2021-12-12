import initSqlJs from "@jlongster/sql.js";
import { SQLiteFS } from "absurd-sql";
import IndexedDBBackend from "absurd-sql/dist/indexeddb-backend";

async function init() {
  let SQL = await initSqlJs({ locateFile: (file) => file });
  let sqlFS = new SQLiteFS(SQL.FS, new IndexedDBBackend());
  SQL.register_for_idb(sqlFS);

  SQL.FS.mkdir("/sql");
  SQL.FS.mount(sqlFS, {}, "/sql");

  if (typeof SharedArrayBuffer === "undefined") {
    let stream = SQL.FS.open(path, "a+");
    await stream.node.contents.readIfFallback();
    SQL.FS.close(stream);
  }

  let db = new SQL.Database("/sql/Northwind_small.sqlite.sqlite3", {
    filename: true,
  });
  db.exec(`
    PRAGMA cache_size=0;
    PRAGMA page_size=4096;
    PRAGMA journal_mode=MEMORY;
  `);
  return db;
}

async function runQueries() {
  let db = await init();

  let stmt = db.prepare(`SELECT * FROM Customer ORDER BY Address LIMIT 10`);
  let fullStart = Date.now();
  let result = stmt.step();
  while (result) {
    let benchmarkData = {},
      benchmark = true;
    if (benchmark === true) {
      benchmarkData.start = Date.now();
    }
    let data = stmt.getAsObject();
    if (benchmark === true) {
      benchmarkData.stop = Date.now();
      console.log(`[${benchmarkData.stop - benchmarkData.start}ms]`);
    }
    console.log(`Result: `, data);
    result = stmt.step();
  }
  let fullStop = Date.now();
  console.log(`Run Time: [${fullStop - fullStart}ms]`);
  stmt.free();
}

runQueries()
  .then((r) => {
    console.log("Finished");
  })
  .catch((err) => {
    console.error(err.message);
  });
