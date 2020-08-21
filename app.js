import { app, query, uuid, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import bodyParser from 'body-parser';
import docker from './docker';
import NetworkMonitor from './network-monitor';
import transitions from './transitions';

const imageName = process.env.MONITOR_IMAGE;

async function monitor() {
  console.log("Starting monitor sync.");
  const runningNetworkMonitors = await NetworkMonitor.findAll("running");
  const runningContainers = await loggedContainers();

  for (let container of runningContainers) {
    let index = runningNetworkMonitors.findIndex((monitor) => monitor.dockerContainer === container.uri);
    if (index > -1) {
      // Already monitoring container, remove from list
      let attachedMonitor = runningNetworkMonitors.splice(index, 1)[0];
      let status = await attachedMonitor.containerStatus();
      console.log(`Status for ${container.uri}: ${status}.`)
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
    transitions.enqueue(await monitor.getLoggedContainer(), monitor, transitions.removeMonitor);
  }
};

async function loggedContainers() {
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
    console.log(`Pulling ${imageName}...`);
    try {
      await docker.pull(imageName);
      console.log('Successfully pulled image.')
      break;
    }
    catch(e) {
      console.error('ERROR: Failed to pull ' + imageName);
    }
  }
}

// Remove all monitors
async function cleanup() {
  console.log("Cleaning up...");
  let monitors = await NetworkMonitor.findAll("running");
  let containers = [];
  for(let monitor of monitors) {
    let container = await monitor.getLoggedContainer();
    containers.push(container);
    transitions.enqueue(container, monitor, transitions.removeMonitor);
  }
  // Wait for all containers to be removed.
  return Promise.all(containers.map(transitions.wait))
                .then(() => console.log("Cleanup done."));
};

// Shut down gracefully
async function cleanAndExit() {
  clearInterval(intervalID);
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
  console.log(`Received delta.`)

  // Assume we always get two delta's, one with only inserts and another with only deletes.
  // We only care about the inserts, assuming that the previous value for docker:state/docker:status was properly deleted
  let inserts;
  try {
    inserts = req.body[0].inserts.length == 0 ? req.body[1].inserts : req.body[0].inserts;
  } catch(error) {
    console.error("ERROR: Got delta in unexpected format.")
    console.error(req.body);
    console.error(error);
  }
  inserts = inserts.filter(triple => triple.predicate.value == "https://w3.org/ns/bde/docker#status");

  for(let change of inserts) {
    let status = change.object.value;
    let container = await getContainerByState(change.subject.value);
    if(container == null) {
      console.error(`ERROR: Could not find container for ${change.subject.value}`);
      return;
    }
    console.log(`Delta: state of ${container.id} changed to ${status}.`)
    if(await isLogged(container)) { // If we're dealing with a container to log
      let monitor = await NetworkMonitor.findByLoggedContainer(container.uri);
      if (status == "running" || status == "created") { // If the container is now running
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
        if(status != "running" && status != "created") { // And it has stopped running
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
    SELECT ?uri ?id ?name ?project
    FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
    WHERE {
      ?uri a docker:Container;
            docker:id ?id;
            docker:name ?name;
            docker:state ${sparqlEscapeUri(state)}.
      OPTIONAL {
        ?uri docker:label ?label.
        ?label docker:key "com.docker.compose.project";
                docker:value ?project.
      }
    }
  `);
  // Assume we only get a single result, as a State object should only be associated with a single container.
  if(result.results.bindings.length > 0) {
    let resultBinding = result.results.bindings[0];
    return {
      uri: resultBinding["uri"].value,
      id: resultBinding["id"].value,
      name: resultBinding["name"].value,
      project: resultBinding["project"] != undefined ? resultBinding["project"].value : undefined,
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
    FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
    WHERE {
      ?uri a docker:Container.
      ${process.env.CAPTURE_CONTAINER_FILTER ? process.env.CAPTURE_CONTAINER_FILTER  : '' }
      FILTER(?uri = ${sparqlEscapeUri(container.uri)})
    }
  `);
  return result.results.bindings.length > 0;
}

process.once("SIGINT", cleanAndExit);
process.once("SIGTERM", cleanAndExit);

// Delta sends messages with Content-Type: application/json rather than application/vnd.api+json
app.use(bodyParser.json());

app.post('/.mu/delta', handleDelta);

let intervalID;
awaitDb().then( () => awaitDocker().then( () => awaitImage().then( () => intervalID = setInterval(monitor, process.env.CAPTURE_SYNC_INTERVAL))));
