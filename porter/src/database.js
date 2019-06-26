import pgRaw from 'pg-promise';

import configs from './config';

const pgPromise = pgRaw();

const { database } = configs;

export const { helpers } = pgPromise;

export const db = pgPromise(database);
