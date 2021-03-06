import { initBackend } from "absurd-sql/dist/indexeddb-main-thread";
import Dexie from "dexie";
import Bottleneck from "bottleneck";
import axios from "axios";
import { UnixFS } from "ipfs-unixfs";
import * as dagPB from "@ipld/dag-pb";
import Buffer from "buffer/";

const cacheLimiter = new Bottleneck({
  maxConcurrent: 2,
});

const bootstraps = [
  "/dns6/ipfs.thedisco.zone/tcp/4430/wss/p2p/12D3KooWChhhfGdB9GJy1GbhghAAKCUR99oCymMEVS4eUcEy67nt",
  "/dns4/ipfs.thedisco.zone/tcp/4430/wss/p2p/12D3KooWChhhfGdB9GJy1GbhghAAKCUR99oCymMEVS4eUcEy67nt",
];

// const IPFS_PUBLIC_GATEWAY = `http://localhost:8080`;
// const IPFS_PUBLIC_GATEWAY = `https://dweb.link`;
const IPFS_PUBLIC_GATEWAY = `https://ipfs.io`;

const ESTUARY_API_URL = `https://api.myfiles.host`;
const ESTUARY_API_TOKEN = `EST38591b75-35ba-49fd-9e94-e2aeee612f30ARY`;

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

// async function saveAuthToken(id, name, token) {
//   let pwdCredential = new PasswordCredential({
//     id: id, // Username/ID
//     name: name, // Display name
//     password: token, // Password
//   });
//
//   return pwdCredential;
// }
//
// async function getAuthToken() {
//   const cred = await navigator.credentials.get();
//
//   return cred;
// }

class IPFSSQLiteDB {
  #DatabaseConfigurationCID;
  #DatabaseConfiguration;

  #databaseData;
  #databaseMetadata;

  #runningCID = null;
  #ipfsClient = null;
  #ipfsErrorCount = 0;

  constructor(DatabaseConfigurationCID) {
    this.#DatabaseConfigurationCID = DatabaseConfigurationCID;
  }

  async #connectToIPFS() {
    if (this.#ipfsClient !== null) {
      return this.#ipfsClient;
    }

    this.#ipfsClient = await Ipfs.create({
      relay: {
        enabled: true,
        hop: {
          enabled: true,
        },
      },
      EXPERIMENTAL: { ipnsPubsub: true, sharding: false },
      config: {
        Addresses: {
          Swarm: [
            "/dns4/star.thedisco.zone/tcp/9090/wss/p2p-webrtc-star",
            "/dns6/star.thedisco.zone/tcp/9090/wss/p2p-webrtc-star",
          ],
        },
      },
    });

    // add bootstraps for next time, and attempt connection just in case we're not already connected
    await this.#dobootstrap(false);

    return this.#ipfsClient;
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
        url: `${IPFS_PUBLIC_GATEWAY}/api/v0/dag/get?arg=${backupConfigurationCID}`,
        responseType: `json`,
        timeout: 5000,
      });
      databaseMetadata = databaseMetadataResult.data;

      console.log(
        `Getting backup from CID: ${databaseMetadata.Versions.Current["/"]}`
      );
      let databaseConfigurationResult = await axios({
        url: `${IPFS_PUBLIC_GATEWAY}/api/v0/dag/get?arg=${databaseMetadata.Versions.Current["/"]}`,
        responseType: `json`,
        timeout: 5000,
      });
      // databaseConfigurationResult.data.Links = databaseConfigurationResult.data.links;
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

  async backup({ databaseName }) {
    //Call Export on DB
    //Get exclusive lock on indexedDB
    //Recursively build links. Either using data to build new CID or using stored CID from previous version
    //Release exclusive lock on indexedDB
    //Build DAG using new Links
    //Publish DAG
    //Pin DAG
    //Confirm File is Pinned
    let ipfsClient = await this.#connectToIPFS();

    let pageCount = (await this.#databaseData.data.count()) - 1;

    let pageCIDs = [];
    for (let pageNumber = 0; pageNumber < pageCount; pageNumber++) {
      pageCIDs[pageNumber] = await this.#savePageToIPFS(pageNumber);
    }

    console.log(pageCIDs);

    let backupFileSettings = {
      type: "file",
      blockSizes: pageCIDs.map(() => {
        return 1024;
      }),
    };
    this.backupFile = new UnixFS(backupFileSettings);
    const cid = await ipfsClient.dag.put(
      dagPB.prepare({
        Data: this.backupFile.marshal(),
        Links: pageCIDs,
      }),
      {
        format: "dag-pb",
        hashAlg: "sha2-256",
      }
    );

    console.log(`Database Backed Up: ${cid.toString()}`);

    let pinRequest = await axios.post(
      `${ESTUARY_API_URL}/pinning/pins`,
      {
        name: databaseName,
        cid: cid.toString(),
      },
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ESTUARY_API_TOKEN}`,
        },
        responseType: `json`,
        timeout: 1000,
      }
    );

    console.log(`Database Pinning: ${JSON.stringify(pinRequest.data)}`);

    let databaseIsPinned = false;
    while (databaseIsPinned === false) {
      let pinCheckRequest = await axios.get(
        `${ESTUARY_API_URL}/pinning/pins/${pinRequest.data.requestid}`,
        {
          headers: {
            Authorization: `Bearer ${ESTUARY_API_TOKEN}`,
          },
          responseType: `json`,
          timeout: 1000,
        }
      );

      if (pinCheckRequest.data.status === "pinned") {
        databaseIsPinned = true;
        console.log(`Database Pinned: ${JSON.stringify(pinCheckRequest.data)}`);
      } else if (pinCheckRequest.data.status === "failed") {
        console.log(
          `Database Pin Failed: ${JSON.stringify(pinCheckRequest.data)}`
        );
      } else {
        console.log(`Database Pin Status: ${pinCheckRequest.data.status}`);
        await sleep(5000);
      }
    }

    return cid;
  }

  async #savePageToIPFS(pageNumber) {
    let ipfsClient = await this.#connectToIPFS();

    let pageData = await this.#databaseData.data.get(pageNumber);

    //Save Page in IPFS
    let uploadedPage = await ipfsClient.add(
      {
        path: `${pageNumber}.page`,
        content: Buffer.Buffer.from(pageData),
      },
      {
        cidVersion: 1,
        pin: false,
      }
    );

    console.log(
      `Uploaded Page: ${pageNumber + 1} at CID [${uploadedPage.cid.toString()}]`
    );

    return uploadedPage.cid;
  }

  // if reconnect is true, it'll first attempt to disconnect from the bootstrap nodes
  async #dobootstrap(reconnect) {
    let ipfsClient = await this.#ipfsClient;

    let now = new Date().getTime();
    if (now - this.lastBootstrap < 60000) {
      // don't try to bootstrap again if we just tried within the last 60 seconds
      return;
    }
    this.lastBootstrap = now;
    for (let bootstrap of bootstraps) {
      if (reconnect) {
        try {
          await ipfsClient.swarm.disconnect(bootstrap);
        } catch (e) {
          console.log(e);
        }
      } else {
        await ipfsClient.bootstrap.add(bootstrap);
      }
      await ipfsClient.swarm.connect(bootstrap);
    }
    return true;
  }

  async #savePageToDB(pageNumber, link) {
    let pageCID = "";

    if (typeof link.Cid !== "undefined") {
      pageCID = link.Cid["/"];
    } else {
      pageCID = link.Hash["/"].toString();
    }

    let pageData = await axios({
      url: `${IPFS_PUBLIC_GATEWAY}/ipfs/${pageCID}`,
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
      url: `${IPFS_PUBLIC_GATEWAY}/api/v0/name/resolve/${
        ipfsCID.split("/")[2]
      }?cacheBust=${cacheBust}`,
      responseType: `json`,
      timeout: 1000, //1 Seconds
    });

    return ipnsResolveRequest.data;

    /*let ipfsClient = await this.#connectToIPFS();
                                        
                                                                                                                                                                                                                    let ipnsResult = null;
                                                                                                                                                                                                                    for await (const name of ipfsClient.name.resolve(ipfsCID)) {
                                                                                                                                                                                                                      console.log(name);
                                                                                                                                                                                                                      ipnsResult = name;
                                                                                                                                                                                                                      return ipnsResult;
                                                                                                                                                                                                                    }*/
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
      } catch (err) {
        console.error(err.message);
      }
      await sleep(15 * 1000);
    }
  }

  async restore(currentBackupConfiguration) {
    if (typeof currentBackupConfiguration === "undefined") {
      currentBackupConfiguration = await this.#refreshDatabaseConfiguration();
    }

    let pageNumber = 0;
    let pageRestoresInProgress = [];
    let pageLinks =
      currentBackupConfiguration.Links || currentBackupConfiguration.links;
    for (let link of pageLinks) {
      let linkString = "";
      if (typeof link.Cid !== "undefined") {
        linkString = link.Cid["/"];
      } else {
        linkString = link.Hash["/"].toString();
      }

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
            size: pageData.data.byteLength * pageLinks.length,
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

// window.testDatabase = new IPFSSQLiteDB(
//   `/ipns/k2k4r8kvtxbn28ei25yc4o1eaisxsjtc17ptwzty6qyal1geiy0bpzat`
// );

window.testDatabase = new IPFSSQLiteDB(
  `/ipns/northwind.myfiles.host/Versions/Current`
);

(async function () {
  let watchDatabase = window.testDatabase.watch(
    `/ipns/northwind.myfiles.host/Versions/Current`
  );
  window.testDatabase.query = async function () {
    try {
      init();
    } catch (err) {
      console.error(err.message);
    }
  };
  // let queryDatabase = (async function () {
  //   let errorCount = 0;
  //   while (errorCount < 50) {
  //     await sleep(10 * 1000);
  //     try {
  //       init();
  //     } catch (err) {
  //       errorCount++;
  //       console.error(err.message);
  //     }
  //   }
  // })();
  await watchDatabase;
  // await queryDatabase;
})();
