import { query, update, sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';

const PREFIXES = `
PREFIX logger:<http://mu.semte.ch/vocabularies/ext/docker-logger/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
`;
class NetworkMonitor {
  constructor({id, uri, status, dockerContainer, persisted}) {
    this.id = id;
    this.uri = uri;
    this.status = status ? status : "created";
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
               logger:monitors ?dockerContainer.
        ${ status ? `BIND(${sparqlEscapeString(status)} as ?status)` : ''}
        }
    `);
    const bindingKeys = result.head.vars;
    const objects =  result.results.bindings.map( (r) => {
      let obj = { persisted: true};
      bindingKeys.forEach((key) => {
        if (r[key])
          obj[key] = r[key].value;
      });
      return new this(obj);
    });
    return objects;
  }

  async remove() {
    await update(`
        ${PREFIXES}
        WITH ${sparqlEscapeUri(process.env.MU_APPLICATION_GRAPH)}
        DELETE {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ?dockerContainer.
        }
        WHERE {
            ${sparqlEscapeUri(this.uri)} a logger:NetworkMonitor;
               mu:uuid ?id;
               logger:status ?status;
               logger:monitors ?dockerContainer.
        }`);
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
               logger:monitors ?dockerContainer,
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
