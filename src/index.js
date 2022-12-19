"use strict";

const url = require("url");

const zlib = require("zlib");
const { promisify } = require("util");
const gunzipAsync = promisify(zlib.gunzip);

const aws = require("aws-sdk");
const Sentry = require("@sentry/serverless");

const s3 = new aws.S3({
  apiVersion: "2006-03-01",
});
const cloudWatchLogs = new aws.CloudWatchLogs({
  apiVersion: "2014-03-28",
});

//specifying the log group and the log stream name for CloudWatch Logs
const logGroupName = process.env.LOG_GROUP_NAME;

if (!logGroupName) {
  console.error("Invalid log group name");
  process.exit(1);
}

const loadBalancerType = process.env.LOAD_BALANCER_TYPE;
const plaintextLogs = process.env.PLAINTEXT_LOGS;

if (process.env.SENTRY_DSN) {
  Sentry.AWSLambda.init({
    dsn: process.env.SENTRY_DSN,
    tracesSampleRate: 0.0,
  });
} else {
  console.log(
    "No SENTRY_DSN environment variable. Skipping Sentry initialization."
  );
}

const MAX_BATCH_SIZE = 1048576; // maximum size in bytes of Log Events (with overhead) per invocation of PutLogEvents
const MAX_BATCH_COUNT = 10000; // maximum number of Log Events per invocation of PutLogEvents
const LOG_EVENT_OVERHEAD = 26; // bytes of overhead per Log Event

const fields = {
  application: [
    "type",
    "time",
    "elb",
    "client:port",
    "target:port",
    "request_processing_time",
    "target_processing_time",
    "response_processing_time",
    "elb_status_code",
    "target_status_code",
    "received_bytes",
    "sent_bytes",
    "request",
    "user_agent",
    "ssl_cipher",
    "ssl_protocol",
    "target_group_arn",
    "trace_id",
    "domain_name",
    "chosen_cert_arn",
    "matched_rule_priority",
    "request_creation_time",
    "actions_executed",
    "redirect_url",
    "error_reason",
    "target:port_list",
    "target_status_code_list",
  ],
  classic: [
    "time",
    "elb",
    "client:port",
    "backend:port",
    "request_processing_time",
    "backend_processing_time",
    "response_processing_time",
    "elb_status_code",
    "backend_status_code",
    "received_bytes",
    "sent_bytes",
    "request",
    "user_agent",
    "ssl_cipher",
    "ssl_protocol",
  ],
  network: [
    "type",
    "version",
    "time",
    "elb",
    "listener",
    "client:port",
    "destination:port",
    "connection_time",
    "tls_handshake_time",
    "received_bytes",
    "sent_bytes",
    "incoming_tls_alert",
    "chosen_cert_arn",
    "chosen_cert_serial",
    "tls_cipher",
    "tls_protocol_version",
    "tls_named_group",
    "domain_name",
    "alpn_fe_protocol",
    "alpn_be_protocol",
    "alpn_client_preference_list",
  ],
};

function portField(fieldName, element, parsed) {
  const field = fieldName.split(":")[0];
  const [ip, port] = element.split(":");
  if (ip === "-1") parsed[field] = parseInt(ip);
  else parsed[field] = ip;

  if (port) parsed[`${field}_port`] = parseInt(port);
  else parsed[`${field}_port`] = -1;
}

// Functions that mutate the parsed object
const fieldFunctions = {
  request: (fieldName, element, parsed) => {
    const [request_method, request_uri, request_http_version] =
      element.split(/\s+/);
    parsed.request_method = request_method;
    parsed.request_uri = request_uri;
    parsed.request_http_version = request_http_version;
    const parsedUrl = url.parse(request_uri);
    parsed.request_uri_scheme = parsedUrl.protocol;
    parsed.request_uri_host = parsedUrl.hostname;
    if (parsedUrl.port) parsed.request_uri_port = parseInt(parsedUrl.port);
    parsed.request_uri_path = parsedUrl.pathname;
    parsed.request_uri_query = parsedUrl.query;
  },
  "target:port": portField,
  "client:port": portField,
  "backend:port": portField,
};

function getS3Object(Bucket, Key) {
  console.log(`Retrieving ${Bucket}/${Key}`);
  return s3
    .getObject({
      Bucket,
      Key,
    })
    .promise();
}

async function createLogGroupIfNotExists() {
  console.log(`Checking Log Group ${logGroupName} exists`);
  const result = await cloudWatchLogs
    .describeLogGroups({
      logGroupNamePrefix: logGroupName,
    })
    .promise();
  if (!result.logGroups[0]) {
    console.log(`Log Group ${logGroupName} creating`);
    await cloudWatchLogs
      .createLogGroup({
        logGroupName,
      })
      .promise();
  } else {
    console.log(`Log Group ${logGroupName} exists`);
  }
}

async function unpackLogData(s3object) {
  console.log(`Unpacking log data for ${loadBalancerType} load balancer`);
  if (loadBalancerType === "classic") return s3object.Body.toString("ascii");
  else {
    const uncompressedLogBuffer = await gunzipAsync(s3object.Body);
    return uncompressedLogBuffer.toString("ascii");
  }
}

function parseLine(line) {
  console.log("Parsing log line");
  const parsed = {};
  let x = 0;
  let end = false;
  let withinQuotes = false;
  let element = "";
  for (const c of line + " ") {
    if (end) {
      if (element) {
        const fieldName = fields[loadBalancerType][x];

        if (element.match(/^\d+.?\d*$/)) element = Number(element);

        if (fieldFunctions[fieldName])
          fieldFunctions[fieldName](fieldName, element, parsed);

        parsed[fieldName] = element;

        element = "";
        x++;
      }
      end = false;
    }

    if (c.match(/^\s$/) && !withinQuotes) end = true;

    if (c === '"') {
      if (withinQuotes) end = true;
      withinQuotes = !withinQuotes;
    } else if (!end) element += c;
  }
  return parsed;
}

async function getLogStreamSequenceToken(logStreamName) {
  console.log(`Checking Log Streams ${logGroupName}/${logStreamName}`);
  let currentStream;
  const cwlDescribeStreams = await cloudWatchLogs
    .describeLogStreams({
      logGroupName,
      logStreamNamePrefix: logStreamName,
    })
    .promise();

  if (cwlDescribeStreams.logStreams[0]) {
    console.log(`Log Stream ${logGroupName}/${logStreamName} exist. Reusing`);
    currentStream = cwlDescribeStreams.logStreams[0];
  } else {
    console.log(`Creating Log Stream ${logGroupName}/${logStreamName}`);
    await cloudWatchLogs
      .createLogStream({
        logGroupName,
        logStreamName,
      })
      .promise();
    const cwlDescribeCreatedStream = await cloudWatchLogs
      .describeLogStreams({
        logGroupName: logGroupName,
        logStreamNamePrefix: logStreamName,
      })
      .promise();
    currentStream = cwlDescribeCreatedStream.logStreams[0];
  }

  return currentStream.uploadSequenceToken;
}

exports.handler = async (event, context) => {
  const logStreamName = context.logStreamName;
  var batches = [];
  var batch = [];
  var batch_size = 0;

  await createLogGroupIfNotExists();

  let sequenceToken = await getLogStreamSequenceToken(logStreamName);

  function readLines(line) {
    // mutate batch, batches, batch_size
    let ts;
    switch (loadBalancerType) {
      case "classic":
        ts = line.split(" ", 1)[0];
        break;
      case "application":
        ts = line.split(" ", 2)[1];
        break;
      case "network":
        ts = line.split(" ", 3)[2];
        break;
      default:
        console.error("Invalid load balancer type");
        process.exit(1);
    }

    const tval = Date.parse(ts);

    if (!plaintextLogs) line = JSON.stringify(parseLine(line));

    const event_size = line.length + LOG_EVENT_OVERHEAD;
    batch_size += event_size;
    if (batch_size >= MAX_BATCH_SIZE || batch.length >= MAX_BATCH_COUNT) {
      // start a new batch
      batches.push(batch);
      batch = [];
      batch_size = event_size;
    }

    batch.push({
      message: line,
      timestamp: tval,
    });
  }

  async function sendBatch(logEvents) {
    // mutate sequenceToken
    console.log(`Sending batch to ${logStreamName}`);
    const putLogEventParams = {
      logEvents,
      logGroupName,
      logStreamName,
      sequenceToken,
    };

    // sort the events in ascending order by timestamp as required by PutLogEvents
    console.log("Sorting events");
    putLogEventParams.logEvents.sort((a, b) => {
      if (a.timestamp > b.timestamp) return 1;
      if (a.timestamp < b.timestamp) return -1;
      return 0;
    });

    console.log("Calling PutLogEvents");
    const cwPutLogEvents = await cloudWatchLogs
      .putLogEvents(putLogEventParams)
      .promise();
    console.log(
      `Success in putting ${putLogEventParams.logEvents.length} events`
    );
    if (
      cwPutLogEvents.rejectedLogEventsInfo?.expiredLogEventEndIndex ||
      cwPutLogEvents.rejectedLogEventsInfo?.tooNewLogEventStartIndex ||
      cwPutLogEvents.rejectedLogEventsInfo?.tooOldLogEventEndIndex
    ) {
      console.warn(
        `Failed in putting events (expiredLogEventEndIndex=${cwPutLogEvents.rejectedLogEventsInfo?.expiredLogEventEndIndex},tooNewLogEventStartIndex=${cwPutLogEvents.rejectedLogEventsInfo?.tooNewLogEventStartIndex},tooOldLogEventEndIndex=${cwPutLogEvents.rejectedLogEventsInfo?.tooOldLogEventEndIndex}`
      );
    }
    sequenceToken = cwPutLogEvents.nextSequenceToken;
  }

  async function sendBatches() {
    batches.push(batch);
    console.log(
      `Finished batching, pushing ${batches.length} batches to CloudWatch`
    );
    let batch_count = 0;
    let count = 0;
    for (let i = 0; i < batches.length; i++) {
      const logEvents = batches[i];
      try {
        if (!logEvents) continue;
        await sendBatch(logEvents);
        ++batch_count;
        count += logEvents.length;
      } catch (err) {
        Sentry.captureException(err);
        console.log("Error sending batch: ", err, err.stack);
        continue;
      }
    }
    batches = [];
    console.log(`Successfully put ${count} events in ${batch_count} batches`);
  }

  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
    const s3object = await getS3Object(bucket, key);
    const logData = await unpackLogData(s3object);
    console.log("Parsing log lines");
    for (const line of logData.trim().split("\n")) {
      readLines(line);
    }
    console.log("Sending batches log lines");
    await sendBatches();
  }
};

if (process.env.SENTRY_DSN) {
  exports.handler = Sentry.AWSLambda.wrapHandler(exports.handler);
}
