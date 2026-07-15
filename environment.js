import env from 'env-var';

export const AUTO_RESTART_MONITOR = env.get('AUTO_RESTART_MONITOR').default("true").asBool();
export const PULL_MONITOR_IMAGE = env.get('PULL_MONITOR_IMAGE').default("true").asBool();
