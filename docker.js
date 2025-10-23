import Docker from 'dockerode';

const dockerode = new Docker({
  socketPath: process.env.CAPTURE_DOCKER_SOCKET,
  version: 'v1.37'
});

function listContainers(options) {
  return new Promise(function(resolve, reject) {
    dockerode.listContainers(options,function(err, containers) {
      if (err)
        reject(err);
      else
        resolve(containers);
    });
  });
};

async function findContainerByName(name) {
  const containers = await listContainers({all: true});

  for( const container of containers ) {
    if( container.Names.includes( name ) )
      return dockerode.getContainer(container.id);
  }
  return null;
}

function pull(image) {
  return new Promise( (resolve, reject) => {
    dockerode.pull(image, function(err, stream) {
      if(err)
        reject(err);
      else {
        dockerode.modem.followProgress(stream, onFinished, function() {});
        function onFinished(err, output) {
          if(err)
            reject(err);
          else
            resolve(output);
        }
      }
    });
  });
};

function remove(container, forceRemove=false) {
  return new Promise( (resolve, reject) => {
    container.remove({force: forceRemove},
                     function (err, data) {
                       if (err) {
                         reject(err);
                       }
                       else {
                         resolve(data);
                       }
                     });
  });
};

function connectContainerTo(containerId, networkName) {
  return new Promise( (resolve, reject) => {
    dockerode.getNetwork(networkName)
             .connect({Container: containerId},
                      (err, data) => {
                        if(err) {
                          reject(err);
                        } else {
                          resolve(data);
                        }
                      });
  });
}

function disconnectContainerFrom(containerId, networkName) {
  return new Promise( (resolve, reject) => {
    dockerode.getNetwork(networkName)
             .disconnect({Container: containerId},
                         (err, data) => {
                           if(err) {
                             reject(err);
                           } else {
                             resolve(data);
                           }
                         });
  });
}

function startContainer(container, opts) {
  return new Promise((resolve, reject) =>
      container.start(opts, (err, data) => {
                           if(err) {
                             reject(err);
                           } else {
                             resolve(data);
                           }
      })
  );
}

const docker = {
  listContainers:  listContainers,
  findContainerByName,
  pull: pull,
  removeContainer: remove,
  createContainer: async (obj) => dockerode.createContainer(obj),
  getContainer: (id) => dockerode.getContainer(id),
  connectContainerTo: connectContainerTo,
  disconnectContainerFrom: disconnectContainerFrom,
  startContainer: startContainer,
};

export default docker;
