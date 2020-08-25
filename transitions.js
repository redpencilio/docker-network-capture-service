import { app, query, uuid, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import NetworkMonitor from './network-monitor';
import docker from './docker';
import events from 'events';

export default {
    enqueue,
    wait,
    createMonitor,
    removeMonitor,
    restartMonitor
};

const imageName = process.env.MONITOR_IMAGE;
const emitter = new events.EventEmitter();
let queue = {};

/**
 * Enqueue a new transition.
 * container and monitor have to be container and monitor *objects*.
 * action is a function.
 */
function enqueue(container, monitor, fun) {
    if(queue[container.id] == undefined) {
        queue[container.id] = {};
    }
    if(queue[container.id].actions == undefined) {
        queue[container.id].actions = [];
    }

    queue[container.id].actions.push({
        fun: fun,
        monitor: monitor
    });

    if(!queue[container.id].processing) {
        queue[container.id].processing = true;
        console.log(`Starting processing for ${container.name}`);
        setTimeout(() => processContainer(container));
    }
}

/**
 * Returns a promise that resolves when the queue for the given container finishes.
 */
function wait(container) {
    return new Promise((resolve, _) => { if(queue[container.id].actions.length == 0) {
                                             resolve();
                                         } else {
                                             emitter.once(container.id, () => resolve())
                                         }
                                       });
}

/**
 * Start processing events for the given container until the queue is empty.
 */
async function processContainer(container) {
    const action = queue[container.id].actions.shift();

    try {
        await action.fun(container, action.monitor);
    } catch(error) {
        console.error(error);
    }

    if(queue[container.id].actions.length > 0) {
        processContainer(container);
    } else {
        queue[container.id].processing = false;
        emitter.emit(container.id);
        console.log(`Finished processing for ${container.name}`);
    }
}

/**
 * Create a new monitor for the given container.
 */
async function createMonitor(container, taskMonitor) {
    const actualMonitor = await NetworkMonitor.findByLoggedContainer(container.uri)
    if(actualMonitor != null || taskMonitor != null) {
        console.error(`Cannot create a monitor for ${container.name}, it already has a monitor ${actualMonitor.id}`);
        return;
    }

    console.log(`Creating monitor for ${container.name}`);
    try {
        const result = await createMonitorContainer(container);
        const monitorContainer = result.monitorContainer;
        const monitor = result.monitor;

        try {
            // The monitor container completely shares its network with the logged container,
            // so to ensure a path to the logstash service we have to add the *logged* container
            // to this network.
            await docker.connectContainerTo(container.id, process.env.LOGSTASH_NETWORK);
        } catch(error) {
            console.error(`ERROR: Failed to connect network to ${container.name}`);
            console.error(error);
            if(error.statusCode != 403) { // 403 = network already connected. Docker API docs don't list this.
                // Try to clean up if we fail creating the monitor properly.
                try {
                    await removeMonitorContainer(monitorContainer, monitor);
                } catch(error) {
                    console.error(error);
                }
                try {
                    await docker.disconnectContainerFrom(container.id, process.env.LOGSTASH_NETWORK);
                } catch(error) {
                    console.log(error);
                }
            }
        }
    }
    catch(error) {
        console.error(`ERROR: Failed to create monitor for ${container.name}`);
        console.error(error);
        return;
    }

    console.log(`Created monitor for ${container.name}`);
}

/**
 * Remove the container's monitor.
 */
async function removeMonitor(loggedContainer, monitor) {
    const actualMonitor = await NetworkMonitor.findByLoggedContainer(loggedContainer.uri)
    if(actualMonitor == null || monitor.id != actualMonitor.id) {
        console.error(`Cannot remove monitor ${monitor.id} as it has already been removed.`);
        return;
    }

    const monitorContainer = docker.getContainer(monitor.id);

    console.log(`Removing monitor for ${loggedContainer.name}: ${monitor.uri}`);

    // Remove the actual container.
    try {
        await removeMonitorContainer(monitorContainer, monitor);
    } catch(error) {
        return; // Just abort, the error message was already printed in the function.
    }

    // Remove the logstash network from the logged container, to prevent errors when adding a new monitor to this container.
    try {
        console.log(`Removing monitor network from ${loggedContainer.name}`);
        console.log(loggedContainer);
        await docker.disconnectContainerFrom(loggedContainer.id, process.env.LOGSTASH_NETWORK);
        console.log(`Removed monitor network from ${loggedContainer.name}`);
    } catch(error) {
        console.error(`Failed removing monitor network from ${loggedContainer.name}`);
        console.error(error);
    }
    console.log(`Removed monitor for ${loggedContainer.name}`);
}

/**
 * Remove and recreate the monitor for the container.
 */
async function restartMonitor(container, taskMonitor) {
    const actualMonitor = await NetworkMonitor.findByLoggedContainer(container.uri)
    if(actualMonitor == null || taskMonitor.id != actualMonitor.id) {
        console.error(`Cannot restart monitor ${taskMonitor.id} as it has been removed.`);
        return;
    }

    const monitorContainer = docker.getContainer(taskMonitor.id);

    console.log(`Restarting monitor for ${container.name}`);
    try {
        // First remove the existing monitor
        await removeMonitorContainer(monitorContainer, taskMonitor);

        // Then create a new monitor
        await createMonitorContainer(container);
    } catch(error) {
        console.error(`Failed restarting monitor for ${container.name}`);
        console.error(error);
        return;
    }

    console.log(`Successfully restarted monitor for ${container.name}`);
}

/**
 * Create and start a new monitor container for the given logged container and network monitor object. Does not touch the network.
 */
async function createMonitorContainer(container) {
    let monitorContainer = null;
    let monitor = null;
    try {
        let containerEnv = ["LOGSTASH_URL=logstash:5044",
                            `DOCKER_ID=${container.id}`,
                            `DOCKER_NAME=${container.name}`,
                            `DOCKER_IMAGE=${container.image}`,
                            `COMPOSE_SERVICE=${await getLabelValue(container, "com.docker.compose.service")}`,
                            `COMPOSE_PROJECT=${await getLabelValue(container, "com.docker.compose.project")}`];
        if(process.env.PACKETBEAT_MAX_MESSAGE_SIZE) {
            containerEnv.push(`PACKETBEAT_MAX_MESSAGE_SIZE=${process.env.PACKETBEAT_MAX_MESSAGE_SIZE}`);
        }
        if(process.env.PACKETBEAT_LISTEN_PORTS) {
            containerEnv.push(`PACKETBEAT_LISTEN_PORTS=${process.env.PACKETBEAT_LISTEN_PORTS}`);
        }
        monitorContainer = await docker.createContainer({
            Image: imageName,
            AttachStdin: false,
            AttachStdout: true,
            AttachStderr: true,
            Labels: { "mu.semte.ch.networkMonitor": container.uri },
            HostConfig: {
                NetworkMode: `container:${container.id}`,
                CapAdd: ["NET_ADMIN", "NET_RAW"]
            },
            Env: containerEnv,
            Tty: false,
            OpenStdin: false,
            StdinOnce: false,
            name: `${container.name}-monitor`
        });
        await docker.startContainer(monitorContainer, {});

        monitor = new NetworkMonitor({
            status: 'running',
            dockerContainer: container.uri,
            id: monitorContainer.id,
            uri: `http://mu.semte.ch/network-monitors/${monitorContainer.id}`
        });
        await monitor.save();
    } catch(error) {
        if(monitorContainer == null) {
            console.error(`ERROR: Failed creating monitor container for ${container.uri}`);
            console.error(error);
        } else {
            console.error(`ERROR: Failed to start monitor for ${container.name}`);
            console.error(error);

            // Clean up to make sure no container is left behind.
            try {
                await docker.removeContainer(monitorContainer, true);
            } catch(error) {
                console.log(error);
            }
        }
        throw(error);
    }
    return { monitorContainer: monitorContainer,
             monitor: monitor
           };
}

/**
 * Remove the given monitor. Does not touch the network.
 */
async function removeMonitorContainer(monitorContainer, monitor) {
    // Try to stop the monitor container first. This will fail if it has already been stopped.
    try {
        console.log(`Stopping monitor container: ${monitorContainer.id}`);
        await monitorContainer.stop({t: 3}); // 3 second deadline for sub-containers.
        console.log(`Stopped monitor container: ${monitorContainer.id}`);
    } catch(error) {
        console.error(`Failed stopping monitor container: ${monitorContainer.id}`);
        console.error(error);
    }
    // Then force remove the monitor container and (only if it succeeds), remove the monitor database entry.
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
        } else {
            console.error(`Failed removing monitor: ${monitor.uri}`);
            throw(error); // If we fail to remove the monitor, throw an error.
        }
    }
}

async function getLabelValue(container, label) {
    const result = await query(`
    PREFIX docker: <https://w3.org/ns/bde/docker#>
    SELECT ?value
    FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
    WHERE {
        ?uri      a            docker:Container;
                  docker:id    ${sparqlEscapeString(container.id)};
                  docker:label ?labelUri.
        ?labelUri a            docker:ContainerLabel;
                  docker:key   ${sparqlEscapeString(label)};
                  docker:value ?value.
    }`);
    if(result.results.bindings.length > 0) {
      return result.results.bindings[0]["value"].value;
    } else {
      console.error(`Cannot find label ${label} for ${container.name}`)
      return null;
    }
}
