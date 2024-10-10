package main

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"net"
	"net/http"

	"time"

	"github.com/aburluka/k8test/internal/fragment"
	bucket "github.com/aburluka/k8test/internal/proto"
	"github.com/aburluka/k8test/internal/registry"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type (
	ApiServer struct {
		bucketRegistry   *registry.Registry
		fragmentRegistry *fragment.Registry
		Router           *mux.Router

		cleanupTicker *time.Ticker
		bucket.UnimplementedApiServiceServer
	}
)

const (
	readChunkSize = 2 << 18
	serverNumber  = 6

	cleanupInterval = 10 * time.Second
)

var (
	log      = logrus.New()
	upgrader = websocket.Upgrader{}
)

func main() {
	server := NewApiServer()

	err := http.ListenAndServe("0.0.0.0:80", server.Router)
	if err != nil {
		log.WithError(err).Fatalln("failed to start web server")
	}
}

func NewApiServer() *ApiServer {
	fr, err := fragment.NewRegistry()
	if err != nil {
		log.WithError(err).Fatalln("failed to create fragment registry")
	}

	s := &ApiServer{
		bucketRegistry:   registry.NewRegistry(),
		fragmentRegistry: fr,
		cleanupTicker:    time.NewTicker(cleanupInterval),
	}

	s.initRouter()
	s.initGRPCServer()

	go s.cleanup()

	return s
}

func (s *ApiServer) initGRPCServer() {
	listener, err := net.Listen("tcp", ":6565")
	if err != nil {
		log.WithError(err).Fatalln("failed to setup gRPC network listener")
	}

	grpcServer := grpc.NewServer()

	bucket.RegisterApiServiceServer(grpcServer, s)
	healthgrpc.RegisterHealthServer(grpcServer, health.NewServer())

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.WithError(err).Fatalln("failed to start gRPC server")
		}
	}()

}

func (s *ApiServer) RegisterBucket(ctx context.Context, r *bucket.RegisterBucketRequest) (*bucket.RegisterBucketResponse, error) {
	b := &registry.Server{
		Address: r.GetAddress(),
	}
	err := s.bucketRegistry.Register(b)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register bucket server: %v", err)
	}

	log.WithField("server", r.GetAddress()).Info("server is registred")

	return &bucket.RegisterBucketResponse{}, nil
}

func (s *ApiServer) deleteFragment(filename, address string, fragment int) error {
	grpcConn, grpcClient, err := s.getBucketServerGRPCClient(address)
	if err != nil {
		return err
	}
	defer grpcConn.Close()

	_, err = grpcClient.DeleteFragment(context.Background(), &bucket.DeleteFragmentRequest{
		Filename: filename,
		Fragment: uint32(fragment),
	})

	return err
}

func (s *ApiServer) cleanup() {
	for range s.cleanupTicker.C {
		for filename, fileInfo := range s.fragmentRegistry.Files {
			if fileInfo.Status != fragment.UploadStatusFailed {
				continue
			}

			log.WithField("filename", filename).Info("cleaning up failed upload")
			for i := range fileInfo.Addresses {
				log.WithField("address", fileInfo.Addresses[i]).Infof("cleaning up %d fragment", i)
				err := s.deleteFragment(filename, fileInfo.Addresses[i], i)
				if err != nil {
					log.WithError(err).Errorf("failed to delete fragment")
					break
				}
			}

			err := s.fragmentRegistry.Delete(filename)
			if err != nil {
				log.WithError(err).Errorf("failed to delete fragment from registry")
				continue
			}

			log.WithField("filename", filename).Info("cleaning up failed upload succeeded")
		}
	}
}

func (s *ApiServer) initRouter() {
	router := mux.NewRouter()

	router.HandleFunc("/upload/{filename}", s.upload)
	router.HandleFunc("/download/{filename}", s.download)

	s.Router = router
}

func (s *ApiServer) getBucketServerGRPCClient(address string) (*grpc.ClientConn, bucket.BucketServiceClient, error) {
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())

	grpcConn, err := grpc.NewClient(address, insecureCreds)
	if err != nil {
		return nil, nil, err
	}

	return grpcConn, bucket.NewBucketServiceClient(grpcConn), nil
}

func (s *ApiServer) chooseServer(filename string, totalBytes int64, payload []byte) *registry.Server {
	bInt64 := make([]byte, 8)

	h := fnv.New64()
	h.Write([]byte(filename))
	binary.LittleEndian.PutUint64(bInt64, uint64(totalBytes))
	h.Write(bInt64)
	h.Write(payload)

	return s.bucketRegistry.GetServer(h)
}

func (s *ApiServer) readUploadFileSize(conn *websocket.Conn) (int64, error) {
	var fileInfo bucket.WsFileInfo
	err := conn.ReadJSON(&fileInfo)
	if err != nil {
		return 0, err
	}

	if fileInfo.GetSize() == 0 {
		return 0, errors.New("empty file")
	}

	return fileInfo.GetSize(), nil
}

func (s *ApiServer) upload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	filename := vars["filename"]
	if len(filename) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Error("can't upgrade connection to websocket")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	fileSize, err := s.readUploadFileSize(conn)
	if err != nil {
		log.WithError(err).Error("failed to read file info")
		return
	}
	chunkSize := fileSize / serverNumber

	log.Infof("uploading file %s with %d size, splitting to chunk with %d", filename, fileSize, chunkSize)

	var (
		totalBytes, currentChunkSize int64
		grpcConn                     *grpc.ClientConn
		grpcClient                   bucket.BucketServiceClient
		bucketServer                 *registry.Server
		grpcStream                   bucket.BucketService_UploadChunksClient
	)

	uploadStatus := fragment.UploadStatusFailed
	defer func() {
		err = s.fragmentRegistry.AddFragment(filename, bucketServer.Address)
		if err != nil {
			log.WithError(err).Error("failed to update registry record")
			return
		}

		err = s.fragmentRegistry.SetStatus(filename, uploadStatus)
		if err != nil {
			log.WithError(err).Errorf("failed to set upload status")
			return
		}
	}()

	defer func() {
		if grpcConn == nil {
			return
		}

		grpcStream.CloseAndRecv()
		grpcConn.Close()
	}()

	fragmentNumber := -1
	for {
		if totalBytes >= fileSize {
			break
		}

		_, b, err := conn.ReadMessage()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.WithError(err).Error("failed to read chunk")
			return
		}

		if len(b) == 0 {
			break
		}

		totalBytes += int64(len(b))
		currentChunkSize += int64(len(b))

		if bucketServer == nil || currentChunkSize >= chunkSize {
			if bucketServer != nil {
				err = s.fragmentRegistry.AddFragment(filename, bucketServer.Address)
				if err != nil {
					log.WithError(err).Error("failed to update registry record")
					return
				}
			}

			fragmentNumber++
			currentChunkSize = 0

			bucketServer = s.chooseServer(filename, totalBytes, b)

			if grpcConn != nil {
				grpcStream.CloseAndRecv()
				grpcConn.Close()
			}

			grpcConn, grpcClient, err = s.getBucketServerGRPCClient(bucketServer.Address)
			if err != nil {
				log.WithError(err).WithField("address", bucketServer.Address).Errorf("failed to init bucket server GRPC client")
				return
			}

			grpcStream, err = grpcClient.UploadChunks(r.Context())
			if err != nil {
				log.WithError(err).Errorf("failed to upload chunks to %s", bucketServer.Address)
				return
			}
		}

		chunk := &bucket.UploadChunk{
			Filename: filename,
			Fragment: uint32(fragmentNumber),
			Chunk: &bucket.Chunk{
				Data: b,
			},
		}

		err = grpcStream.Send(chunk)
		if err != nil {
			log.WithError(err).Errorf("failed to send chunk to %s", bucketServer.Address)
			return
		}
	}

	uploadStatus = fragment.UploadStatusComplete
}

func (s *ApiServer) download(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	filename := vars["filename"]
	if len(filename) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	meta, ok := s.fragmentRegistry.Files[filename]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if meta.Status != fragment.UploadStatusComplete {
		w.WriteHeader(http.StatusNotFound)
		log.WithField("filename", filename).Error("file upload was incomplete")
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Error("can't upgrade connection to websocket")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	var (
		grpcConn   *grpc.ClientConn
		grpcClient bucket.BucketServiceClient
	)

	defer func() {
		if grpcConn != nil {
			grpcConn.Close()
		}
	}()
	for i, server := range meta.Addresses {
		if grpcConn != nil {
			grpcConn.Close()
		}

		grpcConn, grpcClient, err = s.getBucketServerGRPCClient(server)
		if err != nil {
			log.WithError(err).Errorf("failed to connect to %s", server)
			return
		}

		grpcStream, err := grpcClient.DownloadChunks(r.Context(), &bucket.DownloadRequest{
			Filename: filename,
			Fragment: uint32(i),
		})
		if err != nil {
			log.WithError(err).Error("failed to start file fragment downloading")
			return
		}

		for {
			chunk, err := grpcStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				log.WithError(err).Error("failed to download chunk")
				return
			}

			err = conn.WriteMessage(websocket.BinaryMessage, chunk.GetData())
			if err != nil {
				log.WithError(err).Error("failed to sent chunk")
				return
			}
		}
	}

	err = conn.WriteMessage(websocket.BinaryMessage, []byte{})
	if err != nil {
		log.WithError(err).Error("failed to sent chunk")
	}
}
