import { app, query, sparqlEscapeUri } from 'mu';
import bodyParser from 'body-parser';
import docker from './docker';
import NetworkMonitor from './network-monitor';
import transitions from './transitions';

// Env vars
const MONITOR_IMAGE = process.env.MONITOR_IMAGE;
const CAPTURE_SYNC_INTERVAL = process.env.CAPTURE_SYNC_INTERVAL;
const APPLICATION_GRAPH = process.env.MU_APPLICATION_GRAPH;
const CAPTURE_CONTAINER_FILTER = process.env.CAPTURE_CONTAINER_FILTER || '';

let exiting = false;

async function monitor() {
  console.log("Starting monitor sync.");
  const runningNetworkMonitors = await NetworkMonitor.findAll("running");
  console.log(`Found ${runningNetworkMonitors.length} network monitors registered in triplestore`);
  const runningContainers = await loggedContainers();
  console.log(`Found ${runningContainers.length} non-networking containers registered in triplestore`);

  for (let container of runningContainers) {
    let index = runningNetworkMonitors.findIndex((monitor) => monitor.dockerContainer === container.uri);
    if (index > -1) {
      // Already monitoring container, remove from list
      let attachedMonitor = runningNetworkMonitors.splice(index, 1)[0];
      let status = await attachedMonitor.containerStatus();
      console.log(`Status for ${container.uri}: ${status}.`);
      if (status != "running" && status != "created") {
        transitions.enqueue(container, attachedMonitor, transitions.restartMonitor);
      }
    }
    else {
      // not monitoring this container yet, start one
      try {
        transitions.enqueue(container, null, transitions.createMonitor);
      } catch( error ) {
        console.error(`Could not create monitor for ${container.uri}`);
        console.error(error);
      }
    }
  }
  // remaining monitors are for containers that are no longer running or logged, kill them
  for (let monitor of runningNetworkMonitors) {
    const container = await monitor.getLoggedContainer();
    if (container)
      transitions.enqueue(container, monitor, transitions.removeMonitor);
  }
};

async function loggedContainers() {
  const result = await query(`
        PREFIX docker: <https://w3.org/ns/bde/docker#>
        SELECT DISTINCT ?uri ?id ?image ?name
        FROM ${sparqlEscapeUri(APPLICATION_GRAPH)}
        WHERE {
          ?uri a docker:Container;
               docker:id ?id;
               docker:name ?name;
               docker:image ?image;
               docker:state/docker:status "running".
        ${CAPTURE_CONTAINER_FILTER}
        FILTER(NOT EXISTS {
            ?uri docker:label/docker:key "mu.semte.ch.networkMonitor".
          })
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

async function awaitGeneric(successMessage, errorMessage, call) {
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

async function awaitDb() {
  let call = async function () {
    let result = await query('ASK {?s ?p ?o}');
    if (!result.boolean) throw("no triples in the database... whut");
  };
  await awaitGeneric('Successfully connected to database', 'Failed to connect to database', call);
};

async function awaitDocker() {
  await awaitGeneric('Successfully connected to docker daemon', 'Failed to connect to docker daemon', docker.listContainers);
};

async function awaitImage() {
  while (true) {
    console.log(`Pulling ${MONITOR_IMAGE}...`);
    try {
      await docker.pull(MONITOR_IMAGE);
      console.log('Successfully pulled image.');
      break;
    }
    catch(e) {
      console.error('ERROR: Failed to pull ' + MONITOR_IMAGE);
    }
  }
}

// Remove all monitors
async function cleanup() {
  console.log("Cleaning up...");
  let tuples = [];
  for(let monitor of await NetworkMonitor.findAll("running")) {
    tuples.push({
      monitor: monitor,
      container: await monitor.getLoggedContainer()
    });
  }
  // Wait for all containers to be removed.
  return Promise.all(tuples.map(async (tuple) => { transitions.enqueue(tuple.container, tuple.monitor, transitions.removeMonitor);
                                                   return transitions.wait(tuple.container);
                                                 }))
                .then(() => console.log("Cleanup done."));
};

// Shut down gracefully
async function cleanAndExit() {
  clearInterval(intervalID); // Disable sync
  exiting = true; // Stop receiving deltas
  console.log("Signal received.");
  try {
    await cleanup();
    process.exit(0);
  } catch(error) {
    console.error(error);
    process.exit(1);
  }
}

async function handleDelta(req, res) {
  if(exiting) {
    return;
  }

  console.log(`Received delta.`);

  // Assume we always get two delta's, one with only inserts and another with only deletes.
  // We only care about the inserts, assuming that the previous value for docker:state/docker:status was properly deleted
  let inserts;
  try {
    inserts = req.body[0].inserts.length == 0 ? req.body[1].inserts : req.body[0].inserts;
  } catch(error) {
    console.error("ERROR: Got delta in unexpected format.");
    console.error(req.body);
    console.error(error);
  }
  inserts = inserts.filter(triple => triple.predicate.value == "https://w3.org/ns/bde/docker#status");

  const containers = [];
  for(let change of inserts) {
    let status = change.object.value;
    let container = await getContainerByState(change.subject.value);
    container.statusLabel = status;
    if(container == null) {
      console.error(`ERROR: Could not find container for ${change.subject.value}`);
    } else {
      const alreadyFetched = containers.find(c => c.uri == container.uri && c.statusLabel == container.statusLabel);
      if (!alreadyFetched) {
        containers.push(container);
      }
    }
  }
  console.log(`Found ${containers.length} containers in delta for which status got updated`);

  for (let container of containers) {
    console.log(`Delta: state of ${container.id} changed to ${container.statusLabel}.`);
    if(await isLogged(container)) { // If we're dealing with a container to log
      let monitor = await NetworkMonitor.findByLoggedContainer(container.uri);
      if (container.statusLabel == "running" || container.statusLabel == "created") { // If the container is now running
        if(monitor == null) { // And there is no monitor yet
          transitions.enqueue(container, null, transitions.createMonitor); // Create a new monitor
        }
      } else { // If the new status is a non-active status
        if(monitor != null) { // And there's still a monitor
          transitions.enqueue(container, monitor, transitions.removeMonitor); // Remove it
        }
      }
    } else {
      let monitor = await NetworkMonitor.findByRunningContainer(container);
      if(monitor != null) {// If it's a monitoring container.
        if(container.statusLabel != "running" && container.statusLabel != "created") { // And it has stopped running
          let loggedContainer = await monitor.getLoggedContainer();
          transitions.enqueue(loggedContainer, monitor, transitions.restartMonitor); // Restart it
        }
      }
    }
  }
  res.status(200).end();
}

// Return a container by its state URI
async function getContainerByState(state) {
  let result = await query(`
    PREFIX docker: <https://w3.org/ns/bde/docker#>
    SELECT ?uri ?id ?name ?image
    FROM ${sparqlEscapeUri(APPLICATION_GRAPH)}
    WHERE {
      ?uri a docker:Container;
            docker:id ?id;
            docker:name ?name;
            docker:image ?image;
            docker:state ${sparqlEscapeUri(state)}.
    }
  `);
  // Assume we only get a single result, as a State object should only be associated with a single container.
  if(result.results.bindings.length > 0) {
    let resultBinding = result.results.bindings[0];
    return {
      uri: resultBinding["uri"].value,
      id: resultBinding["id"].value,
      name: resultBinding["name"].value,
      image: resultBinding["image"].value,
      status: state
    };
  } else {
    return null;
  }
}

// Check if the container with the given URI is logged.
async function isLogged(container) {
  // This should be an ASK query but I didn't get it to work properly.
  // The CAPTURE_CONTAINER_FILTER environment variable needs the URI
  // of the container bound as ?uri.
  // So we just check if this query returns any results.
  let result = await query(`
    PREFIX docker: <https://w3.org/ns/bde/docker#>
    SELECT ?uri
    FROM ${sparqlEscapeUri(APPLICATION_GRAPH)}
    WHERE {
      ?uri a docker:Container.
      ${CAPTURE_CONTAINER_FILTER}
      FILTER(?uri = ${sparqlEscapeUri(container.uri)})
    }
  `);
  return result.results.bindings.length > 0;
}

process.once("SIGINT", cleanAndExit);
process.once("SIGTERM", cleanAndExit);

// Delta sends messages with Content-Type: application/json rather than application/vnd.api+json
app.post('/.mu/delta', bodyParser.json({ limit: '100mb' }), handleDelta);

let intervalID;

async function init() {
  await awaitDb();
  await awaitDocker();
  await awaitImage();
  intervalID = setInterval( monitor, CAPTURE_SYNC_INTERVAL );
}
await init();
