import { createHash } from 'crypto';
import moment from 'moment';
import { get, set, includes, forIn } from 'lodash';
import Poller from './poller';
import { LICENSE_EXPIRY_SOON_DURATION_IN_DAYS } from './constants';

export default function _xpackInfo(server, pollFrequencyInMillis, clusterSource = 'data') {
  if(!pollFrequencyInMillis) {
    const config = server.config();
    pollFrequencyInMillis = config.get('xpack.xpack_main.xpack_api_polling_frequency_millis');
  }

  let _cachedResponseFromElasticsearch;

  const _licenseCheckResultsGenerators = {};
  const _licenseCheckResults = {};
  let _cachedXPackInfoJSON;
  let _cachedXPackInfoJSONSignature;

  const poller = new Poller({
    functionToPoll: _callElasticsearchXPackAPI,
    successFunction: _handleResponseFromElasticsearch,
    errorFunction: _handleErrorFromElasticsearch,
    pollFrequencyInMillis,
    continuePollingOnError: true
  });

  const xpackInfoObject = {
    license: {
      getUid: function () {
        return get(_cachedResponseFromElasticsearch, 'license.uid');
      },
      isActive: function () {
          return true;        
	//return get(_cachedResponseFromElasticsearch, 'license.status') === 'active';
      },
      expiresSoon: function () {
        //const expiryDateMillis = get(_cachedResponseFromElasticsearch, 'license.expiry_date_in_millis');
        //const expirySoonDate = moment.utc(expiryDateMillis).subtract(moment.duration(LICENSE_EXPIRY_SOON_DURATION_IN_DAYS, 'days'));
        //return moment.utc().isAfter(expirySoonDate);
          return false;
      },
      getExpiryDateInMillis: function () {
        return get(_cachedResponseFromElasticsearch, 'license.expiry_date_in_millis');
      },
      isOneOf: function (candidateLicenses) {
        if (!Array.isArray(candidateLicenses)) {
          candidateLicenses = [ candidateLicenses ];
        }
        return includes(candidateLicenses, get(_cachedResponseFromElasticsearch, 'license.mode'));
      },
      getType: function () {
        return get(_cachedResponseFromElasticsearch, 'license.type');
      }
    },
    feature: function (feature) {
      return {
        isAvailable: function () {
          //return get(_cachedResponseFromElasticsearch, 'features.' + feature + '.available');
            return true;
        },
        isEnabled: function () {
          //return get(_cachedResponseFromElasticsearch, 'features.' + feature + '.enabled');
            return true;
        },
        registerLicenseCheckResultsGenerator: function (generator) {
          _licenseCheckResultsGenerators[feature] = generator;
          _updateXPackInfoJSON();
        },
        getLicenseCheckResults: function () {
          return _licenseCheckResults[feature];
        }
      };
    },
    isAvailable: function () {
      //return !!_cachedResponseFromElasticsearch && !!get(_cachedResponseFromElasticsearch, 'license');
        return true;
    },
    getSignature: function () {
      return _cachedXPackInfoJSONSignature;
    },
    refreshNow: function () {
      const self = this;
      return _callElasticsearchXPackAPI()
      .then(_handleResponseFromElasticsearch)
      .catch(_handleErrorFromElasticsearch)
      .then(() => self);
    },
    stopPolling: function () {
      // This method exists primarily for unit testing
      poller.stop();
    },
    toJSON: function () {
      return _cachedXPackInfoJSON;
    }
  };

  const cluster = server.plugins.elasticsearch.getCluster(clusterSource);

  function _callElasticsearchXPackAPI() {
    server.log([ 'license', 'debug', 'xpack' ], 'Calling Elasticsearch _xpack API');
    return cluster.callWithInternalUser('transport.request', {
      method: 'GET',
      path: '/_xpack'
    });
  };

  function _updateXPackInfoJSON() {
    const json = {};

    // Set response elements common to all features
    set(json, 'license.type', xpackInfoObject.license.getType());
    set(json, 'license.isActive', xpackInfoObject.license.isActive());
    set(json, 'license.expiryDateInMillis', xpackInfoObject.license.getExpiryDateInMillis());

    // Set response elements specific to each feature. To do this,
    // call the license check results generator for each feature, passing them
    // the xpack info object
    forIn(_licenseCheckResultsGenerators, (generator, feature) => {
      _licenseCheckResults[feature] = generator(xpackInfoObject); // return value expected to be a dictionary object
    });
    set(json, 'features', _licenseCheckResults);

    _cachedXPackInfoJSON = json;
    _cachedXPackInfoJSONSignature = createHash('md5')
    .update(JSON.stringify(json))
    .digest('hex');
  }

  function _hasLicenseInfoFromElasticsearchChanged(response) {
    const cachedResponse = _cachedResponseFromElasticsearch;
    return (get(response, 'license.mode') !== get(cachedResponse, 'license.mode')
      || get(response, 'license.status') !== get(cachedResponse, 'license.status')
      || get(response, 'license.expiry_date_in_millis') !== get(cachedResponse, 'license.expiry_date_in_millis'));
  }

  function _getLicenseInfoForLog(response) {
    const mode = get(response, 'license.mode');
    const status = get(response, 'license.status');
    const expiryDateInMillis = get(response, 'license.expiry_date_in_millis');

    return [
      'mode: ' + mode,
      'status: ' + status,
      'expiry date: ' + moment(expiryDateInMillis, 'x').format()
    ].join(' | ');
  }

  function _handleResponseFromElasticsearch(response) {

    if (_hasLicenseInfoFromElasticsearchChanged(response)) {
      let changed = '';
      if (_cachedResponseFromElasticsearch) {
        changed = 'changed ';
      }

      const licenseInfo = _getLicenseInfoForLog(response);
      const logMessage = `Imported ${changed}license information from Elasticsearch for [${clusterSource}] cluster: ${licenseInfo}`;
      server.log([ 'license', 'info', 'xpack'  ], logMessage);
    }

    _cachedResponseFromElasticsearch = response;
    _updateXPackInfoJSON();
  }

  function _handleErrorFromElasticsearch(error) {
    server.log([ 'license', 'warning', 'xpack' ], 'License information could not be obtained from Elasticsearch. ' + error);
    _cachedResponseFromElasticsearch = null;
    _updateXPackInfoJSON();

    // allow tests to shutdown
    error.info = xpackInfoObject;

    throw error;
  }

  // Start polling for changes
  return poller.start()
  .then(() => xpackInfoObject);
}