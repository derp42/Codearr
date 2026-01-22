import axios from "axios";
import http from "http";
import https from "https";

const httpAgent = new http.Agent({ keepAlive: false, maxSockets: 20 });
const httpsAgent = new https.Agent({ keepAlive: false, maxSockets: 20 });

export const httpClient = axios.create({
  timeout: 30000,
  httpAgent,
  httpsAgent,
  maxContentLength: 20 * 1024 * 1024,
  maxBodyLength: 20 * 1024 * 1024,
});
