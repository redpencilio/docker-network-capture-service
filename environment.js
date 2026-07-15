import env from 'env-var';

export const USE_DOCKER_RESTART_POLICY = env.get('USE_DOCKER_RESTART_POLICY').default("false").asBool();
export const PULL_MONITOR_IMAGE = env.get('PULL_MONITOR_IMAGE').default("true").asBool();
