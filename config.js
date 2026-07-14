import env from 'env-var';

export const AUTO_RESTART_MONITOR = env.get('AUTO_RESTART_MONITOR').default("true").asBool();
