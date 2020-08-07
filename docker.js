import Docker from 'dockerode';

const dockerode = new Docker({
  socketPath: process.env.CAPTURE_DOCKER_SOCKET,
  version: 'v1.37'
});

const listContainers = function() {
  return new Promise(function(resolve, reject) {
    dockerode.listContainers(function(err, containers) {
      if (err)
        reject(err);
      else
        resolve(containers);
    });
  });
};

const pull = function(image){
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

const remove = function(container, forceRemove=false) {
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

const connectContainerTo = function(containerId, networkName) {
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

const disconnectContainerFrom = function(containerId, networkName) {
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

const docker = {
  listContainers:  listContainers,
  pull: pull,
  removeContainer: remove,
  createContainer: async (obj) => dockerode.createContainer(obj),
  getContainer: (id) => dockerode.getContainer(id),
  connectContainerTo: connectContainerTo,
  disconnectContainerFrom: disconnectContainerFrom
};

export default docker;
