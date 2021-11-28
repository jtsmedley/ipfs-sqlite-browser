import { initBackend } from 'absurd-sql/dist/indexeddb-main-thread';
import { CID } from "multiformats/cid";
import Dexie from 'dexie';
import _ from 'lodash';
import Bottleneck from "bottleneck";
import { UnixFS } from 'ipfs-unixfs';
import { concat as uint8ArrayConcat } from 'uint8arrays/concat'

const cacheLimiter = new Bottleneck({
  maxConcurrent: 100
});

function init() {
  let worker = new Worker(new URL('./index.worker.js', import.meta.url));
  // This is only required because Safari doesn't support nested
  // workers. This installs a handler that will proxy creating web
  // workers through the main thread
  initBackend(worker);
}

class IPFSSQLiteDB {
  #DatabaseConfigurationCID
  #DatabaseConfiguration

  #backupConfigurationDatabase
  #backupConfigurationMetadata

  #databaseData

  #ipfsClient = undefined;

  constructor(DatabaseConfigurationCID) {
    this.#DatabaseConfigurationCID = DatabaseConfigurationCID;
  }

  async refreshDatabaseConfiguration() {
    if (this.#ipfsClient === undefined) {
      this.#ipfsClient = await window.Ipfs.create();
    }

    this.#DatabaseConfiguration = (await this.#ipfsClient.dag.get(CID.parse(this.#DatabaseConfigurationCID))).value;

    this.#DatabaseConfiguration.Data = (UnixFS.unmarshal(this.#DatabaseConfiguration.Data)).data;
    this.#DatabaseConfiguration.Data = JSON.parse(new TextDecoder().decode(this.#DatabaseConfiguration.Data));

    if (typeof this.#backupConfigurationDatabase === 'undefined') {
      this.#backupConfigurationDatabase = new Dexie(`${this.#DatabaseConfiguration.Data.Name}-CID-Map`);
      this.#backupConfigurationDatabase.version(1).stores({
        pages: 'id'
      });
    }

    if (typeof this.#databaseData === 'undefined') {
      this.#databaseData = new Dexie(`${this.#DatabaseConfiguration.Data.Name}.sqlite3`);
      this.#databaseData.version(.2).stores({
        data: ""
      });
    }

    return this.#DatabaseConfiguration;
  }

  async backup() {
    //Call Export on DB

    //Get exclusive lock on indexedDB

    //Recursively build links. Either using data to build new CID or using stored CID from previous version

    //Release exclusive lock on indexedDB

    //Build DAG using new Links

    //Publish DAG

    //Pin DAG

    //Confirm File is Pinned
  }

  async savePageToDB(pageNumber, link) {
    let pageData = null;
    if (this.#ipfsClient === undefined || true) {
      pageData = await ((await fetch(`http://ipfs.io/ipfs/${link.Hash.toString()}`)).arrayBuffer());
    } else {
      let pageBlocks = [];
      for await (const chunk of this.#ipfsClient.cat(link.Hash.toString())) {
        pageBlocks.push(chunk);
      }
      pageData = uint8ArrayConcat(pageBlocks);
    }

    //Save Page in Database
    let saveReq = this.#databaseData.data.put(await pageData, pageNumber);

    saveReq.then(() => {
      console.log(`Page Number ${pageNumber} Saved to IndexedDB at ${Date.now()}`);
    }).catch((err) => {
      console.error(err);
      throw err;
    })
  }

  async restore(currentBackupConfiguration) {
    if (typeof currentBackupConfiguration === 'undefined') {
      currentBackupConfiguration = await this.refreshDatabaseConfiguration();
    }

    let pageNumber = 0;
    this.#backupConfigurationDatabase.pages.bulkPut(currentBackupConfiguration.Links.map((link) => {
      return {
        id: pageNumber++,
        cid: link.Hash.toString()
      };
    })).then(function(lastKey) {
      console.log("Last page id was: " + lastKey); // Will be 100000.
    }).catch(Dexie.BulkError, function (e) {
      // Explicitely catching the bulkAdd() operation makes those successful
      // additions commit despite that there were errors.
      console.error ("Some pages did not succeed. However, " + e.failures.length + " pages was added unsuccessfully");
    });


    let pageNumber2 = 0;
    let pageRestoresInProgress = [];
    for (let link of currentBackupConfiguration.Links) {
      if (pageNumber2 === 0) {
        let pageData = null;
        if (this.#ipfsClient === undefined) {
          pageData = await ((await fetch(`http://ipfs.io/ipfs/${link.Hash.toString()}`)).arrayBuffer());
        } else {
          let pageBlocks = [];
          for await (const chunk of this.#ipfsClient.cat(link.Hash.toString())) {
            pageBlocks.push(chunk);
          }
          pageData = uint8ArrayConcat(pageBlocks);
        }

        //Save Database File Length
        this.#databaseData.data.put({
          size: (pageData.length * currentBackupConfiguration.Links.length),
          cid: this.#DatabaseConfigurationCID
        }, -1);

        await this.savePageToDB(pageNumber2, link);
      } else if (pageNumber2 < 10 || true) {
        pageRestoresInProgress.push(await cacheLimiter.schedule(() => this.savePageToDB(pageNumber2, link)));
      } else {
        this.#databaseData.data.put(link.Hash.toString(), pageNumber2);
      }
      pageNumber2++;
    }

    await Promise.all(pageRestoresInProgress)

    /*const cache = await caches.open(`${currentBackupConfiguration.Data.Name}`);
    let currentCacheKeys = await cache.keys();
    let urls = currentBackupConfiguration.Links.map((link) => {
      return `http://ipfs.io/ipfs/${link.Hash.toString()}`
    });

    let cachePromises = [];
    for (let url of urls) {
      if (typeof _.find(currentCacheKeys, {
        url: url
      }) === "undefined") {
        console.log(`Caching URL: ${url}`);
         cachePromises.push(cacheLimiter.schedule(() => cache.add(url)));
      }
    }
    await Promise.all(cachePromises);
    console.log(`Cached Database`);*/
  }
}

window.testDatabase = new IPFSSQLiteDB(`bafybeierr7rp2v2ej32u4vmn6rzc7ysch54ts7um55hpo645ea46cky2ae`);

window.testDatabase.restore().then(() => {
  init();
})



