FROM semtech/mu-javascript-template:1.3.1
ENV CAPTURE_DOCKER_SOCKET /var/run/docker.sock
ENV CAPTURE_SYNC_INTERVAL 30000 # miliseconds