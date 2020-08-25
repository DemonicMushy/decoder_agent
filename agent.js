const express = require('express')
const axios = require('axios')
const disk = require('diskusage')
const os = require('os')
const fs = require('fs')
const path = require('path')
const AdmZip = require('adm-zip')
const { getAudioDurationInSeconds } = require('get-audio-duration')

const AWS = require("aws-sdk")
const {BlobServiceClient, StorageSharedKeyCredential} = require("@azure/storage-blob")

const dotenv = require('dotenv')
dotenv.config()

////////////////////////////////////////////////////////////////////////////////
// global variables
////////////////////////////////////////////////////////////////////////////////
const aws_access_key_id       = process.env.AWS_ACCESS_KEY_ID
const aws_secret_access_key   = process.env.AWS_SECRET_ACCESS_KEY
const aws_bucket              = process.env.AWS_BUCKET  // transcription files get uploaded to this bucket

const azure_account           = process.env.AZURE_ACCOUNT
const azure_account_key       = process.env.AZURE_ACCOUNT_KEY
const azure_container         = process.env.AZURE_CONTAINER

const use_storage             = process.env.USE_STORAGE  // aws | azure (azure currently not supported)

const remoteIPv4Url           = 'http://ipv4bot.whatismyipaddress.com/'  // to get external ip
const taskControllerEndpoint  = process.env.TASKCONTROLLER_URL
const intervalPeriod          = process.env.POLLING_PERIOD || 10000 // milliseconds
const port                    = process.env.PORT
const root = os.platform()  === 'win32' ? 'c:' : '/'
const num_bytes_for_buffer    = 100000  // 100kb, buffer space for transcription
var worker_name               = `ipv4address-${os.hostname}`  // ipv4address will be assigned below
const worker_queue            = process.env.WORKER_QUEUE      // default value is 'normal'
const worker_language         = process.env.WORKER_LANGUAGE   // english | mandarin | malay
const worker_sampling_rate    = process.env.WORKER_SAMPLING_RATE ? process.env.WORKER_SAMPLING_RATE.toLowerCase() : '16khz'  // audio sampling rate

const supported_extensions    = ["wav","mp3","mp4","aac","ac3","aiff","amr","flac","m4a","ogg","opus","wma","ts"]
var original_filename         = undefined  // filename received from taskcontroller
var converted_filename        = undefined  // filename after decoder renames (if renaming is enabled)
var isProcessing              = false      // flag to see if currently decoding
var processingInterval        = undefined  // interval to get pending task
var decoderStartTimeout       = undefined  // timeout to check if decoder starts
var putFileInCloud            = undefined  // for switching cloud storage

if (use_storage === 'aws') {
  putFileInCloud = putFileIntoS3
}
else if (use_storage === 'azure') {
  putFileInCloud = putFileIntoAzure
}

getExternalIPv4()  // change worker_name to reflect external ipv4 address
////////////////////////////////////////////////////////////////////////////////
// end of global variables
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// AWS S3 related
////////////////////////////////////////////////////////////////////////////////
AWS.config = new AWS.Config({
  // not needed because it looks at ENV variable already
  // accessKeyId: aws_access_key_id,
  // secretAccessKey: aws_secret_access_key
})

var s3 = (use_storage === 'aws') ? new AWS.S3() : undefined

function putFileIntoS3(key, data) {
  return new Promise( (resolve, reject) => {
    var params = {
      Bucket: aws_bucket,
      Key: key,
      Body: data,
      ACL: "public-read"
      // Metadata: ""
    }
    s3.upload(params, (err, data) => {
      // data.Location = URL of uploaded object
      // data.ETag
      // data.Bucket
      // data.Key
      console.log(`TRANSCRIPT: ${key} uploaded to AWS bucket ${aws_bucket}`)
      resolve(data.Location)
    })
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of AWS S3 related
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// Azure Storage
////////////////////////////////////////////////////////////////////////////////
var containerClient = undefined

if (use_storage === 'azure')  {
  const sharedKeyCredential = new StorageSharedKeyCredential(azure_account, azure_account_key)
  const blobServiceClient = new BlobServiceClient(
    `https://${azure_account}.blob.core.windows.net`,
    sharedKeyCredential
  )
  containerClient = blobServiceClient.getContainerClient(azure_container)
}

async function putFileIntoAzure(key, data) {
  return new Promise( (resolve, reject) => {

    const blockBlobClient = containerClient.getBlockBlobClient(key)

    var objectURL = path.join(containerClient.url, encodeURIComponent(key))

    const uploadBlobResponse = blockBlobClient.upload(data, Buffer.byteLength(data))
    uploadBlobResponse.then(val => {
      console.log(`TRANSCRIPT: ${key} uploaded to Azure storage container ${containerClient.containerName}`)
      resolve(objectURL)
    }).catch(error => {
      console.log(`Error during uploading to Azure: ${error}`)
    })
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of Azure Storage
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////
function getPendingTask(options) {

  console.log(`${new Date()} Checking for pending task...`)
  // make connection to TaskController and check for pending task
  // if pending task available, proceed to download from S3
  var params = {
    type: "reserve",
    worker: worker_name,
    lang: worker_language,
    queue: worker_queue,
    sampling: worker_sampling_rate,
  }

  axios.post(`${taskControllerEndpoint}/tasks/actions`, params)
  .then(response => {
    // response should be a json object according to the specified format
    // get file from S3 based on response

    // if response === null, no pending task
    if (response.data.task) {
      var task = response.data.task
      if (task.queue === worker_queue) {
        console.log("Task received.")
        console.log(response.data)
        retries_count = 0
        handleTask(task)
      }
    }
  })
  .catch(error => {
    console.log(`POLLING_TASK_ERROR: ${error}`)
    sendFailureStatus("POLLING_TASK_ERROR")
  })
}

function sendStatus(status) {

  var params = {
    type: "progress",
    worker: worker_name,
    progress: status
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    console.log(`Status ${status} sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending ${status} status to TaskController: ${error}`)
    console.log(error.response.data)
  })
}

function sendSuccessStatus(cloud_link) {

  var params = {
    type: "success",
    worker: worker_name,
    result: {
      "cloud-link": cloud_link
    }
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    console.log(`Success status sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending success status to TaskController: ${error}`)
  })

  task_id = undefined
  isProcessing = false
}

function sendFailureStatus(error_message) {

  var params = {
    type: "error",
    worker: worker_name,
    err_code: error_message,
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    console.log(`Failure status sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending failure status ${error_message} to TaskController: ${error}`)
  })

  task_id = undefined
  isProcessing = false
  clearTimeout(decoderStartTimeout)
}

function sendRetryRequest() {

  var params = {
    type: "retry",
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    console.log('Retry request sent to TaskController')
  })
  .catch(error => {
    console.log(`Error sending retry request to TaskController: ${error}`)
  })

  task_id = undefined
  isProcessing = false
}
////////////////////////////////////////////////////////////////////////////////
// end of task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// internal function declarations
////////////////////////////////////////////////////////////////////////////////
function initialize() {
  // check for pending task
  processingInterval = setInterval(()=>{
    if (!isProcessing) getPendingTask()
  }, intervalPeriod)
}

function handleTask(task) {

  original_filename = task.data['filename']
  converted_filename = original_filename
  task_id = task.task_id
  var cloud_url = task.data["cloud-link"]

  isProcessing = true

  var meta_info = {
    filename: original_filename,
    formats: task.data.formats,
    numChn: task.data.numChn,
    type: task.data.type
  }
  var ext = path.parse(original_filename).ext.replace(".","")

  if (supported_extensions.includes(ext)) {

    getFileFromURL(cloud_url, original_filename).then( val => {

      decoderStartTimeout = setTimeout(() => {
        cleanUpDecoderFiles()
        sendFailureStatus("DECODER_DID_NOT_START")
      }, 30000)  // 30s wait for decoder to pick up file and start, if not, send failure

      storeMetadataFile(meta_info)
      storeAudioFile(val)
    })
  }
  else {
    sendFailureStatus("FILE_EXTENSION_NOT_SUPPORTED")
  }

}

function getFileFromURL(url, dest) {
  return new Promise((resolve, reject) => {
    axios({
      method: 'get',
      url: url,
      responseType: 'stream'
    })
    .then(response => {

      var content_length = Number(response.headers["content-length"])
      var data = response.data
      var val = {
        filename: dest,
        contentLength: content_length,
        data: data,
      }
      resolve(val)
    })
    .catch(error => {
      console.log(`DOWNLOAD_ERROR: ${error}`)
      sendFailureStatus("DOWNLOAD_ERROR")
    })
  })
}

async function storeAudioFile(val) {
  // need to check if enough storage space before writing to disk
  let avail = await getAvailableSpace()

  var data = val.data
  var contentLength = val.contentLength
  var filename = val.filename

  if (contentLength + num_bytes_for_buffer > avail) {
    // inform about failed decoding
    sendFailureStatus("LOCALDISK_FULL")
    cleanUpDecoderFiles()
  }
  else {
    var file = fs.createWriteStream(`input/${filename}`)
    data.pipe(file)
    console.log(`FILE: Audio file of ${contentLength} bytes saved.`)

    file.on('finish', () => {
      getAudioDurationInSeconds(`input/${filename}`)
      .then(duration => {
        console.log(`Audio file duration: ${Math.floor(duration/60)} minutes ${duration%60} seconds`)
      })
    })

    // file.on('finish', () => {
    //   getAudioDurationInSeconds(`input/${filename}`)
    //   .then(duration => {
    //     console.log(`Audio file duration: ${duration} seconds`)
    //
    //     var timeout_duration = (duration < 90) ? 180 : duration*2
    //     console.log(`Timeout set to ${timeout_duration} seconds`)
    //     decoderStartInterval = setInterval(() => {
    //       cleanUpDecoderFiles()
    //       if (retries_count < 2){
    //         retries_count += 1
    //
    //       }
    //       else {
    //         sendFailureStatus("DECODING_TIMEOUT")
    //       }
    //     }, timeout_duration*1000)  // seconds to milliseconds
    //   })
    // })
  }
}

function storeMetadataFile(val) {

  var name = path.parse(val.filename).name

  var participants = []

  for (var i=1; i < val.numChn+1; i++) {
    participants.push({
      "ChannelId": i,
      "recorder": 1,
      "UserId": i,
      "FarTalkMic": (val.type === "fartalk" || val.type === "boundary"),
      "Transcribe": true,
    })
  }
  if (val.numChn || val.formats) {
    var data = {
      "Participants": participants,
      "output": (val.formats) ? val.formats.map(val=>val.replace('.','')) : undefined
    }

    fs.writeFile(`./input/${name}.txt`, JSON.stringify(data), (error) => {
      if (error) {
        console.log(`Error creating metadata file. ${error}`)
      }
      console.log(`FILE: Metadata file for ${name} saved.`)
    })
  }
  else {
    console.log(`FILE: Metadata file for ${name} was not created.`)
  }

}

function sendTranscriptionFiles(callback) {
  return new Promise( (resolve, reject) => {
    var name = path.parse(converted_filename).name
    var original_name = path.parse(original_filename).name
    fs.readdir(`./output/${name}`, (err, files) => {

      if (files === undefined) {
        sendFailureStatus("TRANSCRIPTIONS_NOT_FOUND")
        reject()
      }

      var zip = new AdmZip()
      files.forEach((itemname) => {
        zip.addLocalFile(`./output/${name}/${itemname}`)
      });

      var zipBuffer = zip.toBuffer()
      callback(`${original_name}.zip`, zipBuffer).then( val => {
        resolve(val)
      })
    })
  })
}

function cleanUpDecoderFiles() {
  // remove audio file
  fs.unlink(`./input/${converted_filename}`, (error) => {
    if (error) console.log(`Error during removal of input file: ${error}`)
    else console.log(`FILE: Input file ${converted_filename} removed.`)
  })

  var name = path.parse(converted_filename).name

  // remove metadata file
  fs.unlink(`./input/${name}.txt`, (error) => {
    if (error) console.log(`Error during removal of metadata file: ${error}`)
    else console.log(`FILE: Metadata file ${name} removed.`)
  })

  // remove details files
  fs.rmdir(`./details/${name}`, {recursive: true}, (error) => {
    if (error) console.log(`Error during removal of details files: ${error}`)
    else console.log(`FILE: Details files for ${name} removed.`)
  })

  // remove transcription files
  fs.rmdir(`./output/${name}`, {recursive: true}, (error) => {
    if (error) console.log(`Error during removal of output files: ${error}`)
    else console.log(`FILE: Output files for ${name} removed.`)
  })
}

async function getAvailableSpace() {
  return new Promise(async (resolve) => {
    var { available } = await disk.check(root)
    console.log(`Local disk availble space: ${available} bytes`)
    resolve(available)
  })
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
const app = express()

// middleware json
app.use(express.json())

app.get('/', (req, res) => {
  res.send("Hello I am alive")
})

app.post('/status', (req, res) => {
  const body = req.body

  converted_filename = body.filename
  var status = body.status
  var channel = body.channel

  res.send("Status received")

  console.log(`DECODER: ${converted_filename} is ${status}.`)
  sendStatus(status)

  status = status.split(" ")[0]
  // upload transcription to AWS S3
  if (status === "DONE") {
    sendTranscriptionFiles(putFileInCloud).then( val => {
      sendSuccessStatus(val)
      cleanUpDecoderFiles()
    })
  }
  else if (status === "STARTING") {
    clearTimeout(decoderStartTimeout)
  }
})

app.post('/error', (req, res) => {
  const body = req.body

  res.send("Error received.")

  var error = body.status

  sendFailureStatus(error)
  cleanUpDecoderFiles()
})

app.get('/stop', (req, res) => {
  clearInterval(processingInterval)
  processingInterval = undefined

  function checkProcessing() {
    if (isProcessing) {
      setTimeout(checkProcessing, 1000)
    }
    else {
      res.send("Processing has finished and will not get new tasks.")
    }
  }
  checkProcessing()
})

app.get('/retry', (req, res) => {
  clearInterval(processingInterval)
  processingInterval = undefined

  if (isProcessing) {
    sendRetryRequest()
  }
})

app.listen(port, () => {
  console.log(`Listening on port ${port}`)
})
////////////////////////////////////////////////////////////////////////////////
// end of http server declaration
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// execution starts here
////////////////////////////////////////////////////////////////////////////////
initialize()
