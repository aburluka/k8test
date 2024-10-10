package main

import (
	"bufio"
	"errors"
	"io"
	"net/url"
	"os"
	"path"

	bucket "github.com/aburluka/k8test/internal/proto"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

const (
	readChunkSize = 2 << 16
)

var (
	log = logrus.New()
)

func main() {
	app := &cli.App{
		Name: "karma-test-client",
		Commands: []*cli.Command{
			upload(),
			download(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatalln("client failed")
	}
}

func upload() *cli.Command {
	return &cli.Command{
		Name:  "upload",
		Usage: "upload a file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "src",
				Value: "./test-file-src.bin",
				Usage: "file to upload",
			},
			&cli.StringFlag{
				Name:  "api-server",
				Value: "0.0.0.0:80",
				Usage: "api server address",
			},
		},
		Action: func(cCtx *cli.Context) error {
			filename := cCtx.String("src")
			u := url.URL{
				Scheme: "ws",
				Host:   cCtx.String("api-server"),
				Path:   path.Join("/upload/", filename),
			}
			log.Infof("connecting to %s", u.String())

			conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				log.WithError(err).Fatalln("failed to connect to api-server")
			}
			defer conn.Close()

			fileInfo, err := os.Stat(filename)
			if err != nil {
				log.WithError(err).WithField("filename", filename).Fatalln("failed to get file info")
			}

			err = conn.WriteJSON(bucket.WsFileInfo{
				Size: fileInfo.Size(),
			})
			if err != nil {
				log.WithError(err).Fatalln("failed to send file info")
			}

			f, err := os.Open(filename)
			if err != nil {
				log.WithError(err).WithField("filename", filename).Fatalln("failed to open file for reading")
			}

			r := bufio.NewReader(f)
			buf := make([]byte, 0, readChunkSize)
			for {
				n, err := io.ReadFull(r, buf[:cap(buf)])
				buf = buf[:n]
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
					log.WithError(err).WithField("filename", filename).Fatalln("failed to read from file")
				}

				err = conn.WriteMessage(websocket.BinaryMessage, buf)
				if err != nil {
					log.WithError(err).Fatalln("failed to sent chunk")
				}
			}

			err = conn.WriteMessage(websocket.BinaryMessage, []byte{})
			if err != nil {
				log.WithError(err).Fatalln("failed to sent terminal message")
			}

			return nil
		},
	}
}

func download() *cli.Command {
	return &cli.Command{
		Name:  "download",
		Usage: "download a file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "src",
				Value: "./test-file-src.bin",
				Usage: "file to download",
			},
			&cli.StringFlag{
				Name:  "dst",
				Value: "./test-file-dst.bin",
				Usage: "local filename for downloaded file",
			},
			&cli.StringFlag{
				Name:  "api-server",
				Value: "0.0.0.0:80",
				Usage: "api server address",
			},
		},
		Action: func(cCtx *cli.Context) error {
			u := url.URL{
				Scheme: "ws",
				Host:   cCtx.String("api-server"),
				Path:   path.Join("/download", cCtx.String("src")),
			}
			log.Infof("connecting to %s", u.String())

			conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				log.WithError(err).Fatalln("failed to connect to api-server")
			}
			defer conn.Close()

			outputFile, err := os.Create(cCtx.String("dst"))
			if err != nil {
				log.WithError(err).Fatalln("failed to create output file")
			}
			defer outputFile.Close()

			for {
				_, b, err := conn.ReadMessage()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					log.WithError(err).Fatal("failed to read chunk")
				}

				if len(b) == 0 {
					break
				}

				_, err = outputFile.Write(b)
				if err != nil {
					log.WithError(err).Fatal("failed to write chunk")
				}
			}

			return nil
		},
	}
}
