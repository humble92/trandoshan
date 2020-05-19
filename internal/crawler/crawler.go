package crawler

import (
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/creekorful/trandoshan/internal/log"
	"github.com/creekorful/trandoshan/internal/natsutil"
	"github.com/creekorful/trandoshan/pkg/proto"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/proxy"
	"mvdan.cc/xurls/v2"
)

type handleCrawl func(nc *nats.Conn, msg *nats.Msg) error

// GetApp return the crawler app
func GetApp() *cli.App {
	return &cli.App{
		Name:    "trandoshan-crawler",
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
				Name:     "tor-uri",
				Usage:    "URI to the TOR SOCKS proxy",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "crawler-count",
				Usage:    "Number of crawlers to run",
				Required: false,
			},
		},
		Action: execute,
	}
}

func execute(ctx *cli.Context) error {
	log.ConfigureLogger(ctx)

	logrus.Infof("Starting trandoshan-crawler v%s", ctx.App.Version)

	logrus.Debugf("Using NATS server at: %s", ctx.String("nats-uri"))
	logrus.Debugf("Using TOR proxy at: %s", ctx.String("tor-uri"))

	dialer, err := proxy.SOCKS5("tcp", ctx.String("tor-uri"), nil, proxy.Direct)
	if err != nil {
		logrus.Errorf("Error creating Tor proxy: %s", err)
	}

	transport := &http.Transport{
		Dial: dialer.Dial,
	}

	client := &http.Client{Transport: transport, Timeout: 8 * time.Second}

	// Create the NATS subscriber
	sub, err := natsutil.NewSubscriber(ctx.String("nats-uri"))
	if err != nil {
		return err
	}
	defer sub.Close()

	logrus.Info("Successfully initialized trandoshan-crawler. Waiting for URLs")

	if ctx.Int("crawler-count") != 0 {
		sub.QueueSubscribeAsync(proto.URLTodoSubject, "crawlers", crawlerWorker, ctx.Int("crawler-count"), client)
	} else {
		if err := sub.QueueSubscribe(proto.URLTodoSubject, "crawlers", handleMessage(client)); err != nil {
			return err
		}
	}

	return nil
}

// Creates a worker that will wait on jobs, used for async crawling
func crawlerWorker(id int, jobs <-chan *nats.Msg, results chan<- int, httpClient *http.Client, nc *nats.Conn) {
	messageHandler := handleMessage(httpClient)
	for {
		job := <-jobs

		var urlMsg proto.URLTodoMsg
		natsutil.ReadJSON(job, &urlMsg)
		logrus.Debugf("Worker %d processing URL: %s", id, urlMsg.URL)

		messageHandler(nc, job)

		// Notifies a crawl was finished
		results <- 0
	}
}

func handleMessage(httpClient *http.Client) natsutil.MsgHandler {
	return func(nc *nats.Conn, msg *nats.Msg) error {
		var urlMsg proto.URLTodoMsg
		if err := natsutil.ReadJSON(msg, &urlMsg); err != nil {
			return err
		}

		var data *http.Response
		var err error

		maxTries := 3
		for i := 1; i <= maxTries; i++ {
			// Query the website
			data, err = httpClient.Get(urlMsg.URL)
			if err != nil {

				// Random TTL expired
				if strings.Contains(err.Error(), "context deadline exceeded") {
					if i == maxTries {
						logrus.Errorf("Error while querying website: %s", err)
						return err
					} else {
						// logrus.Errorf("Retrying URL %s after TTL expire", urlMsg.URL)
						continue
					}
				}

				if i == maxTries {
					logrus.Errorf("Error while querying website: %s", err)
					return err
				}
			}
		}

		body, err := ioutil.ReadAll(data.Body)

		// Publish resource body
		res := proto.ResourceMsg{
			URL:  urlMsg.URL,
			Body: string(body),
		}
		if err := natsutil.PublishJSON(nc, proto.ResourceSubject, &res); err != nil {
			logrus.Errorf("Error while publishing resource body: %s", err)
		}

		// Extract URLs
		xu := xurls.Strict()
		urls := xu.FindAllString(string(body), -1)

		// Publish found URLs
		for _, url := range urls {
			if !strings.Contains(url, ".onion") || strings.Contains(url, ".jpg") || strings.Contains(url, ".css") || strings.Contains(url, ".png") || strings.Contains(url, ".gif") {
				continue
			}

			logrus.Debugf("Found URL: %s", url)

			if err := natsutil.PublishJSON(nc, proto.URLFoundSubject, &proto.URLFoundMsg{URL: url}); err != nil {
				logrus.Errorf("Error while publishing URL: %s", err)
			}
		}

		return nil
	}
}
