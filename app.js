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
        let monitorContainer = await docker.getContainer(attachedMonitor.id);

        // Try to remove the container if it still exists.
        try {
          try {
            console.log("Stopping crashed monitor container...");
            await monitorContainer.stop();
          } catch(error) {
            // Error here means the container is already stopped, which we can ignore.
          }
          console.log("Removing crashed monitor container...");
          await monitorContainer.remove({force: true});
        } catch(error) {
          console.error("Failed to remove container, restarting anyway...");
          console.error(error);
        }

        // Start a new monitor.
        try {
          console.log("Removing crashed monitor...");
          await attachedMonitor.remove();
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
    try {
      await monitor.remove();
      const container = docker.getContainer(monitor.id);
      await container.stop();
      await docker.removeContainer(container);
    } catch( error ) {
      console.error(`Could not clear monitor ${monitor.dockerContainer}`);
      console.error(error);
    }
  }
};

const createMonitorFor = async function(container) {
  console.log('Creating monitor for ' + container.name);
  const monitor = new NetworkMonitor({
    status: 'creating',
    dockerContainer: container.uri,
    path: `share://${container.name.slice(1)}/`
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
      name: `${container.name}-packetbeat`
    });
    try {
      await monitorContainer.start();

      docker
        .getNetwork( "app-http-logger_default" )
        .connect( {
          container: container.id
        }, () => { return; } );

      monitor.status = "running";
      monitor.id = monitorContainer.id;
      monitor.uri = `http://mu.semte.ch/network-monitors/${monitorContainer.id}`;
      await monitor.save();
    }
    catch(error) {
      console.error(`ERROR: Failed to start monitor for ${container.name}`);
      console.error(error);
      await docker.removeContainer(monitorContainer);
    }
  }
  catch(error) {
    console.error(`ERROR: Failed to create monitor for ${container.name}`);
    console.error(error);
  }
};

const cleanup = async function() {
  console.log("Cleaning up...");
  let monitors = await NetworkMonitor.findAll("running");
  // Remove containers asynchronously to ensure we meet the 10s shutdown deadline.
  return Promise.all(
    monitors.map(async function(monitor) {
      const container = docker.getContainer(monitor.id);

      try {
        console.log(`Stopping container: ${container.id}`);
        await container.stop({t: 3}); // 3 second deadline for sub-containers.
      } catch(error) {
        console.log(`Failed stopping container: ${container.id}`);
        console.log(error);
      }

      try {
        console.log(`Removing container: ${container.id}`);
        await container.remove({force: true}); // Force removal in case stopping the container failed.
        console.log(`Removing monitor: ${monitor.id}`);
        await monitor.remove(); // Only remove the monitor after removing its container.
      } catch(error) {
        console.log(error);
      }
    })
  ).then(() => {
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
