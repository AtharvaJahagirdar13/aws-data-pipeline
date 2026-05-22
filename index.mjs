import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { GlueClient, StartJobRunCommand, GetJobRunCommand } from "@aws-sdk/client-glue";

const s3 = new S3Client({ region: "ap-south-1" });
const glue = new GlueClient({ region: "ap-south-1" });
const BUCKET = "csv-raw-data-cc";
const GLUE_JOB_NAME = "superstore-etl-final"; 

export const handler = async (event) => {
  const method = event.requestContext?.http?.method;
  const path = event.requestContext?.http?.path;

  // CORS preflight
  if (method === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
      body: "",
    };
  }

  // Route: GET /status?runId=xxx — check Glue job status
  if (path === "/status") {
    const runId = event.queryStringParameters?.runId;
    const run = await glue.send(new GetJobRunCommand({
      JobName: GLUE_JOB_NAME,
      RunId: runId,
    }));
    const state = run.JobRun.JobRunState; // RUNNING, SUCCEEDED, FAILED
    return {
      statusCode: 200,
      headers: { "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({ state }),
    };
  }

  // Route: GET /?filename=xxx — get presigned URL + trigger Glue
  const filename = event.queryStringParameters?.filename || "file.csv";
  const key = `uploads/${Date.now()}_${filename}`;

  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    ContentType: "text/csv",
  });

  const url = await getSignedUrl(s3, command, { expiresIn: 300 });

  const glueRun = await glue.send(new StartJobRunCommand({
    JobName: GLUE_JOB_NAME,
  }));

  const runId = glueRun.JobRunId;
  console.log(`File: ${key}, Glue RunId: ${runId}`);

  return {
    statusCode: 200,
    headers: { "Access-Control-Allow-Origin": "*" },
    body: JSON.stringify({ url, key, runId }),
  };
};
