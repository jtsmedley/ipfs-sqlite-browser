import { initBackend } from "absurd-sql/dist/indexeddb-main-thread";
import { CID } from "multiformats/cid";
import Dexie from "dexie";
import Bottleneck from "bottleneck";

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
  #databaseWALData;
  #databaseSHMData;
  #databaseMetadata;

  #runningCID = null;

  #ipfsClient = undefined;

  constructor(DatabaseConfigurationCID) {
    this.#DatabaseConfigurationCID = DatabaseConfigurationCID;
  }

  async refreshDatabaseConfiguration(ipfsCID = this.#DatabaseConfigurationCID) {
    if (this.#ipfsClient === undefined) {
      this.#ipfsClient = await window.Ipfs.create();
    }
    let protocol = ipfsCID.split("/")[1];
    let backupConfigurationCID;
    let metadataConfigurationCID;
    debugger;
    if (protocol === "ipns") {
      let metadataResultCID = await this.#resolveIPNS(`${ipfsCID}`);
      metadataConfigurationCID = metadataResultCID.Path.slice(6);
      backupConfigurationCID = metadataConfigurationCID;
    } else if (protocol === "ipfs") {
      backupConfigurationCID = ipfsCID.split("/")[2];
    } else {
      throw new Error(`Invalid Protocol: ${protocol}`);
    }

    let databaseMetadata;
    try {
      //TODO: Timeout handler
      databaseMetadata = (
        await this.#ipfsClient.dag.get(CID.parse(backupConfigurationCID))
      ).value;

      this.#DatabaseConfiguration = (
        await this.#ipfsClient.dag.get(databaseMetadata.Versions.Current)
      ).value;
    } catch (err) {
      console.error(err.message);
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

  async savePageToDB(pageNumber, link) {
    let pageData = null;
    pageData = await (
      await fetch(`http://${link.Hash.toString()}.ipfs.localhost:8080/`)
    ).arrayBuffer();

    //Save Page in Database
    let saveReq = this.#databaseData.data.put(pageData, pageNumber);

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
    let ipnsResolveRequest = await fetch(
      `http://localhost:8080/api/v0/name/resolve/${
        ipfsCID.split("/")[2]
      }?cacheBust=${Date.now()}`
    );
    let ipnsResolveResult = await ipnsResolveRequest.json();
    return ipnsResolveResult;
  }

  async watch(ipnsKey) {
    while (true) {
      let currentCID = (await this.#resolveIPNS(ipnsKey)).Path;
      if (this.#runningCID !== currentCID) {
        try {
          let backupConfiguration = await this.refreshDatabaseConfiguration(
            currentCID
          );
          await this.restore(backupConfiguration);
          this.#runningCID = currentCID;
        } catch (err) {
          console.error(err.message);
        }
      }

      await sleep(5 * 1000);
    }
  }

  async restore(currentBackupConfiguration) {
    if (typeof currentBackupConfiguration === "undefined") {
      currentBackupConfiguration = await this.refreshDatabaseConfiguration();
    }

    let pageNumber = 0;
    let pageRestoresInProgress = [];
    debugger;
    for (let link of currentBackupConfiguration.Links) {
      let linkString = link.Hash.toString();

      let existingLinkString = await this.#databaseMetadata.currentPages.get(
        pageNumber
      );
      if (existingLinkString === linkString) {
        console.log(`Skipping Page ${pageNumber}`);
        pageNumber++;
        continue;
      }

      if (pageNumber === 0) {
        let pageData = null;
        pageData = await (
          await fetch(`http://${linkString}.ipfs.localhost:8080/`)
        ).arrayBuffer();

        //Save Database File Length
        this.#databaseData.data.put(
          {
            size: pageData.byteLength * currentBackupConfiguration.Links.length,
            cid: this.#DatabaseConfigurationCID,
          },
          -1
        );

        await this.savePageToDB(pageNumber, link);
      } else {
        const pageToSave = pageNumber,
          linkToSave = link;
        pageRestoresInProgress.push(
          await cacheLimiter.schedule(() =>
            this.savePageToDB(pageToSave, linkToSave)
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

// window.testDatabase.restore().then(async () => {
//   init();
//   window.testDatabase.watch(
//     `/ipns/k2k4r8kvtxbn28ei25yc4o1eaisxsjtc17ptwzty6qyal1geiy0bpzat`
//   );
//   while (true) {
//     await sleep(10 * 1000);
//     init();
//   }
// });

(async function () {
  window.testDatabase.watch(
    `/ipns/k2k4r8kvtxbn28ei25yc4o1eaisxsjtc17ptwzty6qyal1geiy0bpzat`
  );
  while (true) {
    await sleep(10 * 1000);
    init();
  }
})();
