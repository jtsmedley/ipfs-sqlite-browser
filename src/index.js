import { initBackend } from "absurd-sql/dist/indexeddb-main-thread";
import Dexie from "dexie";
import Bottleneck from "bottleneck";
import axios from "axios";

const cacheLimiter = new Bottleneck({
  maxConcurrent: 100,
});

function init() {
  let worker = new Worker(new URL("./index.worker.js", import.meta.url));
  // This is only required because Safari doesn't support nested
  // workers. This installs a handler that will proxy creating web
  // workers through the main thread
  initBackend(worker);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class IPFSSQLiteDB {
  #DatabaseConfigurationCID;
  #DatabaseConfiguration;

  #databaseData;
  #databaseMetadata;

  #runningCID = null;
  #ipfsErrorCount = 0;

  constructor(DatabaseConfigurationCID) {
    this.#DatabaseConfigurationCID = DatabaseConfigurationCID;
  }

  async #refreshDatabaseConfiguration(
    ipfsCID = this.#DatabaseConfigurationCID
  ) {
    let protocol = ipfsCID.split("/")[1];
    let backupConfigurationCID;
    let metadataConfigurationCID;
    if (protocol === "ipns") {
      let metadataResultCID = await this.#resolveIPNS(`${ipfsCID}`);
      metadataConfigurationCID = metadataResultCID.Path.slice(6);
      backupConfigurationCID = metadataConfigurationCID;
    } else if (protocol === "ipfs") {
      backupConfigurationCID = ipfsCID.split("/")[2];
    } else {
      throw new Error(`Invalid Protocol: ${protocol}`);
    }
    console.log(`Refreshing configuration from CID: ${backupConfigurationCID}`);

    let databaseMetadata;
    try {
      console.log(`Getting metadata from CID: ${backupConfigurationCID}`);
      let databaseMetadataResult = await axios({
        url: `http://localhost:8080/api/v0/dag/get?arg=${backupConfigurationCID}`,
        responseType: `json`,
        timeout: 5000,
      });
      databaseMetadata = databaseMetadataResult.data;

      console.log(
        `Getting backup from CID: ${databaseMetadata.Versions.Current["/"]}`
      );
      let databaseConfigurationResult = await axios({
        url: `http://localhost:8080/api/v0/dag/get?arg=${databaseMetadata.Versions.Current["/"]}`,
        responseType: `json`,
        timeout: 5000,
      });
      databaseConfigurationResult.data.Links =
        databaseConfigurationResult.data.links;
      this.#DatabaseConfiguration = databaseConfigurationResult.data;
    } catch (err) {
      this.#ipfsErrorCount++;

      console.error(err.message);
      return false;
    }

    //Unmarshal Data and Parse JSON
    this.#DatabaseConfiguration.Data = databaseMetadata;

    //Connect to Data Database for SQLite Pages
    if (typeof this.#databaseData === "undefined") {
      this.#databaseData = new Dexie(
        `${this.#DatabaseConfiguration.Data.Name}.sqlite3`
      );
      this.#databaseData.version(0.2).stores({
        data: "",
      });
    }

    //Connect to Metadata Database
    if (typeof this.#databaseMetadata === "undefined") {
      this.#databaseMetadata = new Dexie(
        `${this.#DatabaseConfiguration.Data.Name}.sqlite3-metadata`
      );
      this.#databaseMetadata.version(1).stores({
        currentPages: "",
        changedPages: "",
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

  async #savePageToDB(pageNumber, link) {
    let pageData = await axios({
      url: `http://${
        link.Cid["/"] || link.Hash.toString()
      }.ipfs.localhost:8080/`,
      responseType: `arraybuffer`,
      timeout: 1000, //1 Seconds
    });

    //Save Page in Database
    let saveReq = this.#databaseData.data.put(pageData.data, pageNumber);

    saveReq
      .then(() => {
        console.log(
          `Page Number ${pageNumber} Saved to IndexedDB at ${Date.now()}`
        );
      })
      .catch((err) => {
        console.error(err);
        throw err;
      });
  }

  async #resolveIPNS(ipfsCID) {
    let cacheBust = Math.floor(Date.now() / 1000 / 15); //15 second floored resolution on cachebusts

    let ipnsResolveRequest = await axios({
      url: `http://localhost:8080/api/v0/name/resolve/${
        ipfsCID.split("/")[2]
      }?cacheBust=${cacheBust}`,
      responseType: `json`,
      timeout: 1000, //1 Seconds
    });
    return ipnsResolveRequest.data;
  }

  async watch(ipnsKey) {
    while (true) {
      try {
        console.log(`Checking for changes in IPNS for Key: ${ipnsKey}`);
        let currentCID = (await this.#resolveIPNS(ipnsKey)).Path;
        if (this.#runningCID !== currentCID) {
          console.log(`New Version Found: ${currentCID}`);
          try {
            console.log(`Fetching Latest Configuration`);
            let backupConfiguration = await this.#refreshDatabaseConfiguration(
              currentCID
            );
            if (backupConfiguration === false) {
              throw new Error(`Failed to fetch latest configuration`);
            }
            console.log(`Starting Restore`);
            await this.restore(backupConfiguration);
            console.log(`Completed Restore`);
            this.#runningCID = currentCID;
          } catch (err) {
            console.error(err.message);
          }
        }
        await sleep(5 * 1000);
      } catch (err) {
        console.error(err.message);
      }
    }
  }

  async restore(currentBackupConfiguration) {
    if (typeof currentBackupConfiguration === "undefined") {
      currentBackupConfiguration = await this.#refreshDatabaseConfiguration();
    }

    let pageNumber = 0;
    let pageRestoresInProgress = [];
    for (let link of currentBackupConfiguration.Links) {
      let linkString = link.Cid["/"] || link.Hash.toString();

      let existingLinkString = await this.#databaseMetadata.currentPages.get(
        pageNumber
      );
      if (existingLinkString === linkString) {
        pageNumber++;
        continue;
      }

      if (pageNumber === 0) {
        let pageData = await axios({
          url: `http://${linkString}.ipfs.localhost:8080/`,
          responseType: `arraybuffer`,
          timeout: 1000, //1 Second
        });

        //Save Database File Length
        this.#databaseData.data.put(
          {
            size:
              pageData.data.byteLength *
              currentBackupConfiguration.Links.length,
            cid: this.#DatabaseConfigurationCID,
          },
          -1
        );

        await this.#savePageToDB(pageNumber, link);
      } else {
        const pageToSave = pageNumber,
          linkToSave = link;
        pageRestoresInProgress.push(
          cacheLimiter.schedule(() =>
            this.#savePageToDB(pageToSave, linkToSave)
          )
        );
      }

      this.#databaseMetadata.currentPages.put(linkString, pageNumber);

      pageNumber++;
    }

    await Promise.all(pageRestoresInProgress);
  }
}

window.testDatabase = new IPFSSQLiteDB(
  `/ipns/k2k4r8kvtxbn28ei25yc4o1eaisxsjtc17ptwzty6qyal1geiy0bpzat`
);

(async function () {
  let watchDatabase = window.testDatabase.watch(
    `/ipns/k2k4r8kvtxbn28ei25yc4o1eaisxsjtc17ptwzty6qyal1geiy0bpzat`
  );
  let queryDatabase = (async function () {
    let errorCount = 0;
    while (errorCount < 50) {
      await sleep(10 * 1000);
      try {
        init();
      } catch (err) {
        errorCount++;
        console.error(err.message);
      }
    }
  })();
  await watchDatabase;
  await queryDatabase;
})();
