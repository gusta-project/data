import cosmiconfig from 'cosmiconfig';

const configSearch = cosmiconfig('port').searchSync();

if (configSearch === null) {
  throw new Error(
    'Did not find a config file for module name "port" - see https://github.com/davidtheclark/cosmiconfig#explorersearch'
  );
}

export default configSearch.config;
