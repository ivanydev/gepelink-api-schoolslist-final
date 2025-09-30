const axios = require("axios");

async function fetchKoboData(assetId, token) {
  const url = `https://kf.kobotoolbox.org/api/v2/assets/${assetId}/data/`;
  const res = await axios.get(url, {
    headers: {
      Authorization: `Token ${token}`
    }
  });
  return res.data.results;
}

module.exports = { fetchKoboData };
