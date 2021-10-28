const express = require("express");
const axios = require("axios");
const disk = require("diskusage");
const os = require("os");
const fs = require("fs");
const path = require("path");
const AdmZip = require("adm-zip");
const queue = require("queue");

const dotenv = require("dotenv");
dotenv.config();

////////////////////////////////////////////////////////////////////////////////
// global variables
////////////////////////////////////////////////////////////////////////////////

const sharedVolumeDir = process.env.SHARED_VOLUME_MOUNT;

const remoteIPv4Url = "http://ipv4bot.whatismyipaddress.com/"; // to get external ip
const taskControllerEndpoint = process.env.TASKCONTROLLER_URL;
const decoderStartWaitTime = process.env.DECODER_START_WAIT_TIME || 30000;  // milliseconds
const intervalPeriod = process.env.POLLING_PERIOD || 15000; // milliseconds
const port = process.env.PORT;
const root = os.platform() === "win32" ? "c:" : "/";
const num_bytes_for_buffer = 100000; // 100kb, buffer space for transcription
const inputDir = path.join(sharedVolumeDir, 'input')
const outputDir = path.join(sharedVolumeDir, 'output')
// const logsDir = sharedVolumeDir + '/logs'
const detailsDir = path.join(sharedVolumeDir, 'details')
const transcriptionDestination = path.join(sharedVolumeDir, "transcriptions") // output folder for transcriptions, meant to be docker volume mapped to existing location

var worker_name = `ipv4address-${os.hostname}`; // ipv4address will be assigned below
const worker_queue = process.env.WORKER_QUEUE; // default value is 'normal'
const worker_language = process.env.WORKER_LANGUAGE;
const worker_sampling_rate = process.env.WORKER_SAMPLING_RATE
  ? process.env.WORKER_SAMPLING_RATE.toLowerCase()
  : "16khz"; // audio sampling rate


const supported_extensions = [
  "wav",
  "mp3",
  "mp4",
  "aac",
  "ac3",
  "aiff",
  "amr",
  "flac",
  "m4a",
  "ogg",
  "opus",
  "wma",
  "ts",
];
var original_filename = undefined; // filename received from taskcontroller
var converted_filename = undefined; // filename after decoder renames (if renaming is enabled)
var outgoingRequestsQueue = queue({ concurrency: 1, autostart: true }); // queue for outgoing requests to TaskController, to prevent race conditions
var isProcessing = true; // flag to see if currently decoding
var processingInterval = undefined; // interval to get pending task
var decoderStartTimeout = undefined; // timeout to check if decoder starts

getExternalIPv4(); // change worker_name to reflect external ipv4 address
////////////////////////////////////////////////////////////////////////////////
// end of global variables
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////
function getPendingTask(options) {
  // make connection to TaskController and check for pending task
  var params = {
    type: "reserve",
    worker: worker_name,
    lang: worker_language,
    queue: worker_queue,
    sampling: worker_sampling_rate,
  };

  axios
    .post(`${taskControllerEndpoint}/tasks/actions`, params)
    .then((response) => {
      // if response === null, no pending task
      if (response.data.task) {
        console.log(`${new Date()}`);
        var task = response.data.task;
        if (task.queue === worker_queue) {
          console.log("Task received.");
          console.log(response.data);
          isProcessing = true;
          retries_count = 0;
          handleTask(task);
        }
      }
    })
    .catch((error) => {
      console.log(`TASK_OBJECT_ERROR: ${error}`);
      outgoingRequestsQueue.push((cb) =>
        sendFailureStatus("TASK_OBJECT_ERROR", cb)
      );
    });
}

function sendStatus(status, callback) {
  // queue requires a callback
  var params = {
    type: "progress",
    worker: worker_name,
    progress: status,
  };

  axios
    .post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
    .then((response) => {
      console.log(`TASKCONTROLLER: Status ${status} sent to TaskController.`);
    })
    .catch((error) => {
      console.log(
        `TASKCONTROLLER: Error sending ${status} status to TaskController: ${error}`
      );
      console.log(error.response.data);
    })
    .finally(callback);
}

function sendSuccessStatus(file_path, callback) {
  // queue requires a callback
  var params = {
    type: "success",
    worker: worker_name,
    result: {
      "file_path": file_path,
    },
  };

  axios
    .post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
    .then((response) => {
      console.log(`TASKCONTROLLER: Success status sent to TaskController.`);
    })
    .catch((error) => {
      console.log(
        `TASKCONTROLLER: Error sending success status to TaskController: ${error}`
      );
    })
    .finally(callback);

  task_id = undefined;
  isProcessing = false;
}

function sendFailureStatus(error_message, callback) {
  // queue requires a callback
  var params = {
    type: "error",
    worker: worker_name,
    err_code: error_message,
  };

  axios
    .post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
    .then((response) => {
      console.log(
        `TASKCONTROLLER: Failure status ${error_message} sent to TaskController.`
      );
    })
    .catch((error) => {
      console.log(
        `TASKCONTROLLER: Error sending failure status ${error_message} to TaskController: ${error}`
      );
    })
    .finally(callback);

  task_id = undefined;
  isProcessing = false;
  clearTimeout(decoderStartTimeout);
}

function sendRetryRequest(callback) {
  // queue requires a callback
  var params = {
    type: "retry",
  };

  axios
    .post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
    .then((response) => {
      console.log("TASKCONTROLLER: Retry request sent to TaskController");
    })
    .catch((error) => {
      console.log(
        `TASKCONTROLLER: Error sending retry request to TaskController: ${error}`
      );
    })
    .finally(callback);

  task_id = undefined;
  isProcessing = false;
}
////////////////////////////////////////////////////////////////////////////////
// end of task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// internal function declarations
////////////////////////////////////////////////////////////////////////////////
function initialize() {
  // empty input folder
  emptyInputFolder();
  // check for pending task
  processingInterval = setInterval(() => {
    if (!isProcessing) getPendingTask();
  }, intervalPeriod);
}

function handleTask(task) {
  original_filename = task.data["filename"];
  converted_filename = original_filename;
  task_id = task.task_id;
  var cloud_url = task.data["cloud-link"];

  var meta_info = {
    filename: original_filename,
    formats: task.data.formats,
    numChn: task.data.numChn,
    type: task.data.type,
  };
  var ext = path.parse(original_filename).ext.replace(".", "");

  if (supported_extensions.includes(ext)) {
    saveMetadataFile(meta_info);
    saveFileFromPath(cloud_url, original_filename).then(() => {
      decoderStartTimeout = setTimeout(() => {
        cleanUpDecoderFiles();
        outgoingRequestsQueue.push((cb) =>
          sendFailureStatus("DECODER_DID_NOT_START", cb)
        );
      }, decoderStartWaitTime); // 30s wait for decoder to pick up file and start, if not, send failure
    });
  } else {
    outgoingRequestsQueue.push((cb) =>
      sendFailureStatus("FILE_EXTENSION_NOT_SUPPORTED", cb)
    );
  }
}

function saveFileFromPath(filePath, dest) {
  // return axios({
  //   method: "get",
  //   url: url,
  //   responseType: "stream",
  // })
  var readStream = fs.createReadStream(path.join(sharedVolumeDir, 'gateway_uploads', path.parse(filePath).base));
  var contentLength = fs.statSync(filePath).size;
  var promiseResolved = false
  Promise.resolve({ data: readStream, contentLength: contentLength })
    .then((response) => {
      return new Promise(async (resolve, reject) => {
        var content_length = Number(response.contentLength);

        let avail = await getAvailableSpace();

        if (content_length + num_bytes_for_buffer > avail) {
          // inform about failed decoding
          outgoingRequestsQueue.push((cb) =>
            sendFailureStatus("LOCALDISK_FULL", cb)
          );
          cleanUpDecoderFiles();
          reject();
        } else {
          var file = fs.createWriteStream(inputDir + `/${dest}`);
          response.data.pipe(file);

          let error = null;

          file.on("error", (err) => {
            error = err;
            console.log(`SAVING_AUDIOFILE_ERROR: ${error}`);
            outgoingRequestsQueue.push((cb) =>
              sendFailureStatus("SAVING_AUDIOFILE_ERROR", cb)
            );
            file.close();
            reject();
          });

          file.on("close", () => {
            if (!error) {
              console.log(`FILE: Audio file of ${content_length} bytes saved.`);
              resolve();
            }
          });
        }
        promiseResolved = true
      });
    })
    .catch((err) => {
      promiseResolved = false
      console.log(`DOWNLOAD_ERROR: ${err}`);
      outgoingRequestsQueue.push((cb) =>
        sendFailureStatus("DOWNLOAD_ERROR", cb)
      );
    });

    if(promiseResolved) {
      return Promise.resolve()
    } else {
      return Promise.reject()
    }
}

function saveMetadataFile(val) {
  var name = path.parse(val.filename).name;

  var participants = [];

  for (var i = 1; i < val.numChn + 1; i++) {
    participants.push({
      ChannelId: i,
      recorder: 1,
      UserId: i,
      FarTalkMic: val.type === "fartalk" || val.type === "boundary",
      Transcribe: true,
    });
  }
  if (val.numChn || val.formats) {
    var data = {
      Participants: participants,
      output: val.formats
        ? val.formats.map((val) => val.replace(".", ""))
        : undefined,
    };

    fs.writeFile(`${inputDir}/${name}.txt`, JSON.stringify(data), (error) => {
      if (error) {
        console.log(`FILE: Error creating metadata file. ${error}`);
      } else {
        console.log(`FILE: Metadata file for ${name} saved.`);
      }
    });
  } else {
    console.log(`FILE: Metadata file for ${name} was not created.`);
  }
}

function sendTranscriptionFiles(callback) {
  return new Promise((resolve, reject) => {
    var name = path.parse(converted_filename).name;
    var original_name = path.parse(original_filename).name;
    fs.readdir(`${outputDir}/${name}`, (err, files) => {
      if (files === undefined) {
        outgoingRequestsQueue.push((cb) =>
          sendFailureStatus("TRANSCRIPTIONS_NOT_FOUND", cb)
        );
        reject();
      }

      var zip = new AdmZip();
      files.forEach((itemname) => {
        zip.addLocalFile(`${outputDir}/${name}/${itemname}`);
        console.log(`${outputDir}/${name}/${itemname} added`)
      });

      var zipBuffer = zip.toBuffer();
      callback(`${original_name}.zip`, zipBuffer).then((val) => {
        resolve(val);
      });
    });
  });
}

function saveFile(key, data) {
  return new Promise((resolve, reject)=> {
    var fullpath = path.join(transcriptionDestination, key)
    fs.writeFile(fullpath, data, (err) => {
      if (err) throw err;

      console.log(`TRANSCRIPT: ${key} written to ${fullpath}`)
      resolve(fullpath)
    })
  })
}

function cleanUpDecoderFiles() {
  // remove audio file
  fs.unlink(`${inputDir}/${converted_filename}`, (error) => {
    if (error)
      console.log(`CLEANUP: Error during removal of input file: ${error}`);
    else console.log(`CLEANUP: Input file ${converted_filename} removed.`);
  });

  var name = path.parse(converted_filename).name;

  // remove metadata file
  fs.unlink(`${inputDir}/${name}.txt`, (error) => {
    if (error)
      console.log(`CLEANUP: Error during removal of metadata file: ${error}`);
    else console.log(`CLEANUP: Metadata file ${name} removed.`);
  });

  // remove details files
  fs.rmdir(`${detailsDir}/${name}`, { recursive: true }, (error) => {
    if (error)
      console.log(`CLEANUP: Error during removal of details files: ${error}`);
    else console.log(`CLEANUP: Details files for ${name} removed.`);
  });

  // remove transcription files
  fs.rmdir(`${outputDir}/${name}`, { recursive: true }, (error) => {
    if (error)
      console.log(`CLEANUP: Error during removal of output files: ${error}`);
    else console.log(`CLEANUP: Output files for ${name} removed.`);
  });
}

function emptyInputFolder() {
  // clean input folder, to be called once upon initialize
  console.log("Attempting to empty input folder of contents...");
  fs.readdir(`${inputDir}`, (err, files) => {
    if (files === undefined) {
      console.log("Input folder already empty. Doing nothing...");
    } else {
      files.forEach((itemname) => {
        fs.unlinkSync(`${inputDir}/${itemname}`);
        console.log(`Input folder item ${itemname} removed.`);
      });
    }
    isProcessing = false;
  });
}

async function getAvailableSpace() {
  return new Promise(async (resolve) => {
    var { available } = await disk.check(root);
    console.log(`Local disk availble space: ${available} bytes`);
    resolve(available);
  });
}

// Try getting an external IPv4 address.
async function getExternalIPv4(debug = false) {
  try {
    const response = await axios.get(remoteIPv4Url);
    if (response && response.data) {
      worker_name = `${response.data}-${os.hostname}`;
    }
  } catch (error) {
    if (debug) {
      console.log("Error getting external ipv4 address.");
      console.log(error);
    }
  }
}
////////////////////////////////////////////////////////////////////////////////
// end of internal function declarations
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// http server declaration
////////////////////////////////////////////////////////////////////////////////
const app = express();

// middleware json
app.use(express.json());

app.get("/", (req, res) => {
  if (isProcessing) {
    res.send("Hello I am alive and currently processing a file.");
  } else {
    res.send("Hello I am alive and currently NOT processing a file.");
  }
});

app.post("/status", (req, res) => {
  const body = req.body;

  converted_filename = body.filename;
  var status = body.status;
  // var channel = body.channel

  res.send("Status received");

  console.log(`DECODER: ${converted_filename} is ${status}.`);
  outgoingRequestsQueue.push((cb) => sendStatus(status, cb));

  status = status.split(" ")[0];
  // upload transcription to AWS S3
  if (status === "DONE") {
    sendTranscriptionFiles(saveFile).then((val) => {
      outgoingRequestsQueue.push((cb) => sendSuccessStatus(val, cb));
      cleanUpDecoderFiles();
    });
  } else if (status === "STARTING") {
    if (decoderStartTimeout._destroyed) {
      setTimeout(() => {
        clearTimeout(decoderStartTimeout);
      }, 5000); // wait 5s in case decoderStartTimeout is set after receiving signal
    } else {
      clearTimeout(decoderStartTimeout);
    }
  }
});

app.post("/error", (req, res) => {
  const body = req.body;

  res.send("Error received.");

  var error = body.status;

  outgoingRequestsQueue.push((cb) => sendFailureStatus(error, cb));
  cleanUpDecoderFiles();
});

app.get("/stop", (req, res) => {
  clearInterval(processingInterval);
  processingInterval = undefined;

  function checkProcessing() {
    if (isProcessing) {
      setTimeout(checkProcessing, 1000);
    } else {
      res.send("Processing has finished and will not get new tasks.");
    }
  }
  checkProcessing();
});

app.get("/retry", (req, res) => {
  clearInterval(processingInterval);
  processingInterval = undefined;

  if (isProcessing) {
    outgoingRequestsQueue.push((cb) => sendRetryRequest(cb));
  }
});

app.listen(port, () => {
  console.log(`Listening on port ${port}`);
});
////////////////////////////////////////////////////////////////////////////////
// end of http server declaration
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// handling of SIGINT
////////////////////////////////////////////////////////////////////////////////
process.on("SIGINT", () => {
  console.log("Stopping...");
  if (isProcessing) {
    outgoingRequestsQueue.push((cb) =>
      sendFailureStatus("CONTAINER_SHUTDOWN", cb)
    );
  }
  process.exit(0);
});
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// execution starts here
////////////////////////////////////////////////////////////////////////////////
initialize();