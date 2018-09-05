import fs from 'fs-extra';
import { app, query, uuid, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import docker from './docker';
import NetworkMonitor from './network-monitor';

const imageName = 'crccheck/tcpdump';
const shareToPath = function(share) {
  return share.replace(/^share:\/\//,'/data/');
};

const containers = async function() {
  const result = await query(`
        PREFIX docker: <https://w3.org/ns/bde/docker#>
        SELECT ?uri ?id ?name ?project
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ?uri a docker:Container;
               docker:id ?id;
               docker:name ?name;
               docker:state/docker:status "running".
        OPTIONAL {
            ?uri docker:label ?label.
            ?label docker:key "com.docker.compose.project";
                   docker:value ?project.
        }
        ${process.env.CAPTURE_CONTAINER_FILTER ? process.env.CAPTURE_CONTAINER_FILTER  : '' }
        FILTER(
          NOT EXISTS {
             ?uri docker:label ?networkLabel.
             ?networkLabel docker:key "mu.semte.ch.networkMonitor".
          }
        )
        }
    `);
  const bindingKeys = result.head.vars;
  const objects =  result.results.bindings.map( (r) => {
    let obj = {};
    bindingKeys.forEach((key) => {
      if (r[key])
        obj[key] = r[key].value;
    });
    return obj;
  });
  return objects;
};



function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
};

const awaitGeneric = async function(successMessage, errorMessage, call) {
  let delay = 1;
  while (true) {
    try {
      const list = await call();
      console.info(successMessage);
      return;
    }
    catch(e) {
      console.warn(e);
      console.warn(`ERROR: ${errorMessage}, waiting ${delay} seconds and retrying`);
      await timeout(1000*delay);
      delay = delay * 2;
    }

  }
}
const awaitDb = async function() {
  let call = async function () {
    let result = await query('ASK {?s ?p ?o}');
    if (!result.boolean) throw("no triples in the database... whut");
  };
  await awaitGeneric('successfully connected to database', 'failed to connect to database', call);
};
const awaitDocker = async function() {
  await awaitGeneric('successfully connected to docker daemon', 'failed to connect to docker daemon', docker.listContainers);
};


const createMonitorFor = async function(container) {
  console.log('creating monitor for ' + container.name);
  const monitor = new NetworkMonitor({
    status: 'running',
    dockerContainer: container.uri,
    path: `share://${container.name.slice(1)}/`
  });
  await fs.mkdirp(shareToPath(monitor.path));
  await fs.chmod(shareToPath(monitor.path), 0o777 ); // TODO: fix this, currently because of a bad mix of host and namespaced containers
  try {
    const monitorContainer = await docker.createContainer({
      Image: imageName,
      AttachStdin: false,
      AttachStdout: true,
      AttachStderr: true,
      Labels: { "mu.semte.ch.networkMonitor": monitor.dockerContainer},
      HostConfig: {
        NetworkMode: `container:${container.id}`,
        Binds: [`${process.env.PCAP_VOLUME}:/data`]
      },
      Tty: false,
      Cmd: ["-i","any", '-w', `${shareToPath(monitor.path)}\%FT\%H\%M\%S-${container.name.slice(1)}.pcap`, "-G 60"],
      OpenStdin: false,
      StdinOnce: false,
      name: `${container.name}-tcpdump`
    });
    try {
      await monitorContainer.start();
      monitor.status = "started";
      monitor.id = monitorContainer.id;
      monitor.uri = `http://mu.semte.ch/network-monitors/${monitorContainer.id}`;
      await monitor.save();
    }
    catch(error) {
      console.log(`ERROR starting monitor for ${container.name}`);
      console.log(error);
      await docker.removeContainer(monitorContainer);
    }
  }
  catch(error) {
    console.log(`ERROR: failed to create monitor for ${container.name}`);
    console.log(error);
  }
};
const monitorAllTheThings = async function() {
  const runningContainers = await containers();
  const runningNetworkMonitors = await NetworkMonitor.findAll();
  for (let container of runningContainers) {
    let index = runningNetworkMonitors.findIndex((monitor) => monitor.dockerContainer === container.uri);
    if (index > -1) {
      // already monitoring container, remove from list
      runningNetworkMonitors.splice(index, 1);
    }
    else {
      // not monitoring this container yet, start one
      await createMonitorFor(container);
    }
  }
  // remaining monitors are for containers that are no longer running, kill them
  for (let monitor of runningNetworkMonitors) {
    await monitor.remove();
    const container = docker.getContainer(monitor.id);
    await container.stop();
    await docker.removeContainer(container);
  }
};

var pulledTCPDump = false;

const cleanup = async function() {
  for (let monitor of await NetworkMonitor.findAll()) {
    console.debug(`removing monitor: ${monitor.id}`);
    await monitor.remove();
    const container = docker.getContainer(monitor.id);
    try {
      await container.stop();
    }
    catch(e) {}
    finally {
      try {
        await docker.removeContainer(container);
      }
      catch(e) {}
    }
  }
  console.debug('cleanup done');
};

const program = async function() {
  // wait for the docker endpoint and sparql endpoint to be available
  await awaitDb();
  await awaitDocker();
  if (!pulledTCPDump) {
    console.log('pulling latest tcpdump');
    try {
      await docker.pull(imageName);
    }
    catch(e) {
      console.error('ERROR: FAILED TO PULL ' + imageName);
    }
    pulledTCPDump = true;
  }
  // sync docker state to db
  await monitorAllTheThings();
  setTimeout(program, process.env.CAPTURE_SYNC_INTERVAL);
};

awaitDb().then( () => awaitDocker().then( () => cleanup().then ( () => program())));

