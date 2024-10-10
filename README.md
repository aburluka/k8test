# karma8 task

## Description & Notes

Brief architecture looks like:

Client <==WebSocket==> API server <==GRPC==> * Bucket server(s)

Client can download/upload required file via quite minimalistic protocol:

* Upload: `[API server address]/upload/{filename}` endpoint 
```
          1. Send file info
          2. Send data chunk one by one
          3. Send empty chunk
```

* Download: `[API server address]/upload/{filename}` endpoint
```
          1. Receive data chunks
          2. Receive empty chunk
```

An API server performs WS interaction with clients, keeps file fragment
registry in `fragments.json` file and performs registration&simplistic load-balancing amongst
bucket servers, using consistent hashing algo.

**NOTE** *: Text file for registry was used in sake of simplicity*

**NOTE** *: Hashing function could use some improvements, sometimes it's not smooth enough*

If uploading is abnormally interrupted and WS connection closed, whole upload 
is marked as failed and cleanup process will ask specific bucket server to
remove already stored fragments.

A bucket server has no in memory state and perfoms all operations directly with FS.

**NOTE** *: a lot of room for impovement: connection break with an API server is not handled,*
*registration can send data about alread stored fragments and so on*

Style & coding issues:
* zero tests implemented, only manual testing was performed, using CLI client.
* a lot of hardcoded values
* no configuration files
* Initialization is ugly, DI should help
* Logs are a mess

### Prerequisites

* Go 1.22
    * Refer to go.mod to see all dependencies
* Docker
* Docker-compose

## Docker Compose
Docker Compose will allow you to run this project locally.
Please keep the service versions in docker-compose.yml in sync with the versions used in bitbucket-pipelines.yml.

### Running Docker Compose

1. Ensure that you have Docker running locally first
2. Run the following command - `make images` to build required images
3. Run the following command - `docker-compose up -d`, this will run docker as a background task

Alternatively, you can run everything using the makefile, by running the following command -
`make up`

and clean with the following command -
`make down`

## CLI Client Usage

### Generate test data

`make generate-test-file`

or one can easily generate large test file, using `dd` command, e.g. 

`dd of=test-file-src.bin bs=100M count=1 if=/dev/urandom`

### Upload file

`./bin/client upload -src=test-file-src.bin`

### Download

`./bin/client upload -src=test-file-src.bin -dst=test-file-dst.bin`

### Testing

Typical testing could look like:
```
make all
make images
msessionake up
make generate-test-file
./bin/client upload
./bin/client download
md5sum test-file-src.bin test-file-dst.bin
```
Feel free to interrupt upload process, to see how cleanup works.

