package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"

	"github.com/aburluka/k8test/internal/fragment"
	bucket "github.com/aburluka/k8test/internal/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type (
	BucketServer struct {
		apiServerConn        *grpc.ClientConn
		apiServiceGRPCClient bucket.ApiServiceClient
		fragmentStorage      *fragment.Storage

		bucket.UnimplementedBucketServiceServer
	}
)

const (
	readChunkSize = 2 << 16
)

var (
	log               = logrus.New()
	address           *string
	apiServerAddress  *string
	fragmentDirectory *string
)

func init() {
	address = flag.String("address", "0.0.0.0:6571", "bucket server address")
	apiServerAddress = flag.String("api-server", "0.0.0.0:6565", "API server address")
	fragmentDirectory = flag.String("fragments", "fragments", "fragments storage directory")
}

func main() {
	flag.Parse()

	s, err := NewBucketServer()
	if err != nil {
		log.WithError(err).Fatalln("failed to initialize bucket server")
	}
	defer s.shutdown()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func NewBucketServer() (*BucketServer, error) {
	var err error

	s := &BucketServer{}

	s.fragmentStorage, err = fragment.NewFragmentsStorage(*fragmentDirectory)
	if err != nil {
		return nil, err
	}

	err = s.initGRPCClient()
	if err != nil {
		return nil, err
	}

	err = s.initGRPCServer()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *BucketServer) UploadChunks(stream bucket.BucketService_UploadChunksServer) error {
	var (
		b        bytes.Buffer
		filename string
		fragment int
	)

	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.WithError(err).Error("failed to recv chunk")
			return err
		}

		b.Write(request.Chunk.Data)
		filename = request.GetFilename()
		fragment = int(request.GetFragment())
	}

	err := s.fragmentStorage.Put(filename, fragment, b.Bytes())
	if err != nil {
		log.WithError(err).Error("failed to put fragment")
		return err
	}
	
	log.WithFields(logrus.Fields{"filename": filename, "fragment": fragment}).Info("fragment stored")

	return nil
}

func (s *BucketServer) DownloadChunks(r *bucket.DownloadRequest, stream bucket.BucketService_DownloadChunksServer) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "failed to download chunks - invalid gRPC request")
	}

	f, err := s.fragmentStorage.Get(r.Filename, int(r.Fragment))
	if err != nil {
		return status.Error(codes.NotFound, "failed to download chunks - fragment is not found")
	}

	log.WithFields(logrus.Fields{"filename": r.GetFilename(), "fragment": r.GetFragment()}).Info("downloading fragment")

	reader := bufio.NewReader(f)
	buf := make([]byte, 0, readChunkSize)
	for {
		n, err := io.ReadFull(reader, buf[:cap(buf)])
		buf = buf[:n]
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.WithError(err).WithField("filename", r.Filename).Fatalln("failed to read from file")
		}

		chunk := &bucket.Chunk{
			Data: buf,
		}
		err = stream.Send(chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *BucketServer) DeleteFragment(ctx context.Context, r *bucket.DeleteFragmentRequest) (*bucket.DeleteFragmentResponse, error) {
	log.WithFields(logrus.Fields{"filename": r.GetFilename(), "fragment": r.GetFragment()}).Info("deleting fragment")

	err := s.fragmentStorage.Delete(r.GetFilename(), int(r.GetFragment()))
	if err != nil {
		return nil, err
	}

	return &bucket.DeleteFragmentResponse{}, nil
}

func (s *BucketServer) initGRPCClient() error {
	insecureCreds := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.NewClient(*apiServerAddress, insecureCreds)
	if err != nil {
		return fmt.Errorf("failed to connect to API server: %w", err)
	}

	s.apiServerConn = conn

	grpcClient := bucket.NewApiServiceClient(s.apiServerConn)

	_, err = grpcClient.RegisterBucket(context.Background(), &bucket.RegisterBucketRequest{Address: *address})

	if err != nil {
		return fmt.Errorf("failed to register bucket server: %w", err)
	}

	return nil
}

func (s *BucketServer) initGRPCServer() error {
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return fmt.Errorf("failed to setup gRPC network listener: %w", err)
	}

	grpcServer := grpc.NewServer()

	bucket.RegisterBucketServiceServer(grpcServer, s)
	healthgrpc.RegisterHealthServer(grpcServer, health.NewServer())

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.WithError(err).Fatalln("failed to start gRPC server")
		}
	}()

	return nil
}

func (s *BucketServer) shutdown() {
	if s.apiServerConn != nil {
		s.apiServerConn.Close()
	}
}
