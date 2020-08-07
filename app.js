import fs from 'fs-extra';
import { app, query, uuid, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import docker from './docker';
import NetworkMonitor from './network-monitor';

const imageName = process.env.MONITOR_IMAGE;

const monitor = async function() {
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
        // The monitor crashed, restart it.
        console.log(`Restarting monitor for ${container.uri}`);

        // Try to remove the container if it still exists.
        await removeMonitor(attachedMonitor);

        // Start a new monitor.
        try {
          console.log("Starting new monitor...");
          await createMonitorFor(container);
          console.log(`Monitor restarted for ${container.uri}`);
        } catch(error) {
          console.error(`Could not restart monitor for ${container.uri}`);
          console.error(error);
        }
      }
    }
    else {
      // not monitoring this container yet, start one
      try {
        await createMonitorFor(container);
      } catch( error ) {
        console.error(`Could not create monitor for ${container.uri}`);
        console.error(error);
      }
    }
  }
  // remaining monitors are for containers that are no longer running or logged, kill them
  for (let monitor of runningNetworkMonitors) {
    removeMonitor(monitor);
  }
};

const createMonitorFor = async function(container) {
  console.log('Creating monitor for ' + container.name);
  const monitor = new NetworkMonitor({
    status: 'creating',
    dockerContainer: container.uri,
  });
  try {
    const monitorContainer = await docker.createContainer({
      Image: imageName,
      AttachStdin: false,
      AttachStdout: true,
      AttachStderr: true,
      Labels: { "mu.semte.ch.networkMonitor": monitor.dockerContainer },
      HostConfig: {
        NetworkMode: `container:${container.id}`,
        CapAdd: ["NET_ADMIN", "NET_RAW"]
      },
      Env: ["LOGSTASH_URL=logstash:5044",
            `COMPOSE_PROJECT=${container.project}`,
            `COMPOSE_SERVICE=${container.name}`,
            `COMPOSE_CONTAINER_ID=${container.id}`],
      Tty: false,
      OpenStdin: false,
      StdinOnce: false,
      name: `${container.name}-monitor`
    });
    try {

      // The monitor container completely shares its network with the logged container,
      // so to ensure a path to the logstash service we have to add the *logged* container
      // to this network.
      await docker.connectContainerTo(container.id, process.env.LOGSTASH_NETWORK);

      await monitorContainer.start();

      monitor.status = "running";
      monitor.id = monitorContainer.id;
      monitor.uri = `http://mu.semte.ch/network-monitors/${monitorContainer.id}`;
      await monitor.save();
    }
    catch(error) {
      console.error(`ERROR: Failed to start monitor for ${container.name}`);
      console.error(error);

      // Clean up to make sure no connection or network is left behind.
      try {
        await docker.removeContainer(monitorContainer, true);
      } catch(error) {}
      try {
        await docker.disconnectContainerFrom(container.id, process.env.LOGSTASH_NETWORK);
      } catch(error) {}
    }
  }
  catch(error) {
    console.error(`ERROR: Failed to create monitor for ${container.name}`);
    console.error(error);
  }
};

const removeMonitor = async function(monitor) {
  const monitorContainer = docker.getContainer(monitor.id);
  const loggedContainer = await monitor.loggedContainerId();

  console.log(`Removing monitor: ${monitor.uri}`);

  // Try to stop the monitor container first. This will fail if it has already been stopped.
  try {
    console.log(`Stopping monitor container: ${monitorContainer.id}`);
    await monitorContainer.stop({t: 3}); // 3 second deadline for sub-containers.
    console.log(`Stopped monitor container: ${monitorContainer.id}`);
  } catch(error) {
    console.error(`Failed stopping monitor container: ${monitorContainer.id}`);
    console.error(error);
  }

  // Remove the logstash network from the logged container, to prevent errors when adding a new monitor to this container.
  try {
    console.log(`Removing monitor network from ${loggedContainer}`);
    await docker.disconnectContainerFrom(loggedContainer, process.env.LOGSTASH_NETWORK);
    console.log(`Removed monitor network from ${loggedContainer}`);
  } catch(error) {
    console.error(`Failed removing monitor network from ${loggedContainer}`);
    console.error(error);
  }

  // Finally, force remove the monitor container and (only if it succeeds), remove the monitor database entry.
  try {
    console.log(`Removing monitor container: ${monitorContainer.id}`);
    await docker.removeContainer(monitorContainer, true); // Force removal in case stopping the container failed.
    console.log(`Removed monitor container: ${monitor.id}`);
    await monitor.remove(); // Only remove the monitor after removing its container.
    console.log(`Removed monitor: ${monitor.uri}`);
  } catch(error) {
    console.error(`Failed removing monitor container: ${monitorContainer.id}`);
    console.error(error);
    if(error.statusCode == 404) { // 404 = "no such container", e.g. the container is already gone and we can safely set the monitor to "removed"
      await monitor.remove();
      console.log(`Removed monitor: ${monitor.uri}`);
    }
  }
}

const cleanup = async function() {
  console.log("Cleaning up...");
  let monitors = await NetworkMonitor.findAll("running");
  // Remove containers asynchronously to ensure we meet the 10s shutdown deadline.
  return Promise.all(monitors.map(removeMonitor))
                .then(() => {
                  console.log("Cleanup done.");
                });
};

const loggedContainers = async function() {
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
  await awaitGeneric('Successfully connected to database', 'Failed to connect to database', call);
};

const awaitDocker = async function() {
  await awaitGeneric('Successfully connected to docker daemon', 'Failed to connect to docker daemon', docker.listContainers);
};

const awaitImage = async function() {
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

// Shut down gracefully
const cleanAndExit = async function() {
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

process.once("SIGINT", cleanAndExit);
process.once("SIGTERM", cleanAndExit);

let intervalID = undefined;
awaitDb().then( () => awaitDocker().then( () => awaitImage().then( () => intervalID = setInterval(monitor, process.env.CAPTURE_SYNC_INTERVAL))));
