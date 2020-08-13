import { query, update, sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';

const PREFIXES = `
PREFIX logger:<http://mu.semte.ch/vocabularies/ext/docker-logger/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
`;
class NetworkMonitor {
  constructor({id, uri, status, dockerContainer, persisted}) {
    this.id = id;
    this.uri = uri;
    this.status = status ? status : "running";
    this.dockerContainer = dockerContainer;
    this._persisted = persisted ? persisted : false;
  }

  static async findAll(status=null) {
    const result = await query(`
        ${PREFIXES}
        SELECT ?id ?uri ?status ?dockerContainer
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ?uri a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ?dockerContainer
        ${ status ? `FILTER(?status = ${sparqlEscapeString(status)})` : ''}
        }
    `);
    const bindingKeys = result.head.vars;
    const objects =  result.results.bindings.map( (r) => {
      let obj = { persisted: true };
      bindingKeys.forEach((key) => {
        if (r[key])
          obj[key] = r[key].value;
      });
      return new this(obj);
    });
    return objects;
  }

  // Relate a container URI to the network monitor monitoring that container.
  static async findByLoggedContainer(containerURI) {
    const result = await query(`
        ${PREFIXES}
        SELECT ?id ?uri ?status
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ?uri a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ${sparqlEscapeUri(containerURI)}.
          FILTER(?status = "running")
        }
    `);
    if(result.results.bindings.length > 0) {
      const binding = result.results.bindings[0];
      return new this({
        persisted: true,
        id: binding["id"].value,
        uri: binding["uri"].value,
        status: binding["status"].value,
        dockerContainer: containerURI
      });
    } else {
      return null;
    }
  }

  // Find the container running this network monitor
  static async findByRunningContainer(container) {
    const result = await query(`
        ${PREFIXES}
        SELECT ?uri ?status ?dockerContainer
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ?uri a logger:NetworkMonitor;
               mu:uuid ${sparqlEscapeString(container.id)};
               logger:status ?status;
               logger:monitors ?dockerContainer.
        }
    `);
    if(result.results.bindings.length > 0) {
      const binding = result.results.bindings[0];
      return new this({
        persisted: true,
        id: container.id,
        uri: binding["uri"].value,
        status: binding["status"].value,
        dockerContainer: binding["dockerContainer"].value
      });
    } else {
      return null;
    }
  }
 
  async remove() {
    this.status = "removed";
    this.save();
  }

  async getLoggedContainer() {
    const result = await query(`
        ${PREFIXES}
        PREFIX docker: <https://w3.org/ns/bde/docker#>
        SELECT ?id ?name ?status
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ${sparqlEscapeUri(this.dockerContainer)} a docker:Container;
                                                     docker:id ?id;
                                                     docker:name ?name;
                                                     docker:state/docker:status ?status.
        }

    `);
    if(result.results.bindings.length > 0) {
      const binding = result.results.bindings[0];
      return {
        uri: this.dockerContainer,
        id: binding["id"].value,
        name: binding["name"].value,
        status: binding["status"].value
      };
    } else {
      return null;
    }
  }

  // Fetch the status of the container this network monitor runs on.
  async containerStatus() {
    const result = await query(`
        PREFIX docker: <https://w3.org/ns/bde/docker#>
        SELECT ?status
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ?uri a docker:Container;
               docker:id ${sparqlEscapeString(this.id)};
               docker:state/docker:status ?status.
        }
    `);
    if(result.results.bindings.length > 0) {
      return result.results.bindings[0]["status"].value;
    } else {
      console.error(`Cannot find status for ${this.id}`)
      return "none";
    }
  }

  async loggedContainerId() {
    const result = await query(`
        PREFIX docker: <https://w3.org/ns/bde/docker#>
        SELECT ?id
        FROM ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        WHERE {
          ${sparqlEscapeUri(this.dockerContainer)} a docker:Container;
                                                     docker:id ?id.
        }
    `);
    if(result.results.bindings.length > 0) {
      return result.results.bindings[0]["id"].value;
    } else {
      console.error(`Cannot find logged container id for ${this.id}`)
      return "none";
    }
  }

  async save() {
    if ( ! this._persisted ) {
      await this._create();
    }
    else {
      await this._update();
    }
  }

  async _create() {
    await update(`
        ${PREFIXES}
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)} {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
                                    mu:uuid ${sparqlEscapeString(this.id)};
                                    logger:status ${sparqlEscapeString(this.status)};
                                    logger:monitors ${sparqlEscapeUri(this.dockerContainer)}.
          }
       }
   `);
  }
  async _update() {
    await update(`
        ${PREFIXES}
        WITH ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        DELETE {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ?dockerContainer.
        }
        INSERT {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
                                    mu:uuid ${sparqlEscapeString(this.id)};
                                    logger:status ${sparqlEscapeString(this.status)};
                                    logger:monitors ${sparqlEscapeUri(this.dockerContainer)}.
        }
        WHERE {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ?dockerContainer.
        }
    `);
  }
}

export default NetworkMonitor;
