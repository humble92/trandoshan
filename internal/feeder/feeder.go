package feeder

import (
	"bufio"
	"os"

	"github.com/creekorful/trandoshan/internal/log"
	"github.com/creekorful/trandoshan/internal/natsutil"
	"github.com/creekorful/trandoshan/pkg/proto"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

// GetApp return the feeder app
func GetApp() *cli.App {
	return &cli.App{
		Name:    "trandoshan-feeder",
		Version: "0.0.1",
		Usage:   "", // TODO
		Flags: []cli.Flag{
			log.GetLogFlag(),
			&cli.StringFlag{
				Name:     "nats-uri",
				Usage:    "URI to the NATS server",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "url",
				Usage:    "URL to send to the crawler",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "file",
				Usage:    "File to send all urls to the crawler",
				Required: false,
			},
		},
		Action: execute,
	}
}

func execute(ctx *cli.Context) error {
	log.ConfigureLogger(ctx)

	logrus.Infof("Starting trandoshan-feeder v%s", ctx.App.Version)

	logrus.Debugf("Using NATS server at: %s", ctx.String("nats-uri"))

	// Connect to the NATS server
	nc, err := nats.Connect(ctx.String("nats-uri"))
	if err != nil {
		logrus.Errorf("Error while connecting to NATS server %s: %s", ctx.String("nats-uri"), err)
		return err
	}
	defer nc.Close()

	if ctx.String("url") != "" {
		// Publish single message
		if err := natsutil.PublishJSON(nc, proto.URLTodoSubject, &proto.URLTodoMsg{URL: ctx.String("url")}); err != nil {
			logrus.Errorf("Unable to publish URL: %s", err)
			return err
		}

		logrus.Infof("URL %s successfully sent to the crawler", ctx.String("url"))
	} else if ctx.String("file") != "" {
		// Publish all urls in file
		file, err := os.Open(ctx.String("file"))

		if err != nil {
			logrus.Errorf("Failed to open file url file %s", err)
			return err
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if err := natsutil.PublishJSON(nc, proto.URLTodoSubject, &proto.URLTodoMsg{URL: scanner.Text()}); err != nil {
				logrus.Errorf("Unable to publish URL: %s", err)
			}

			logrus.Infof("URL %s successfully sent to the crawler", ctx.String("url"))
		}
	} else {
		// Did not specify single url or file
		logrus.Errorf("Did not specify a url or file to start")
	}

	return nil
}
