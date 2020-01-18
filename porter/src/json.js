import { join } from 'path';
import glob from 'glob-promise';
import { readFile } from 'jsonfile';

import loggers from './logging';

const log = loggers('json');

export const readAllFiles = async directory => {
  const results = [];

  try {
    const matches = await glob(join(`${directory}`, '*.json'));

    for (const match of matches) {
      const data = await readFile(match);

      results.push.apply(results, data);
    }
  } catch (err) {
    log.error(err);
  }

  return results;
};
