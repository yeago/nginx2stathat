package main

import (
	"flag"
	"fmt"
	"github.com/ActiveState/tail"
	"github.com/hgfischer/nginx2stathat/loghit"
	"github.com/stathat/go"
	"log/syslog"
	"os"
	"net/url"
	"strings"
)

// Command line flags and arguments
var (
	statPrefix	 = flag.String("prefix", "", "Stat prefix. Ex.: \"`hostname -s` live site\"")
	parserRoutines = flag.Int("parsers", 4, "Number of parallel routines parsing log lines and queueing them to the posters")
	posterRoutines = flag.Int("posters", 4, "Number of parallel routines sending results to StatHat")
	dryrun	   = flag.Bool("dryrun", false, "Whether to actually create the stats or just print them")
	ezKey		  string
	accessLog	  string
)

// Print command line help and exit application
func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] [EZ Key] [access.log]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
	os.Exit(1)
}

// Parse command line
func parseCommandLine() {
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "Error: Missing arguments\n")
		flag.Usage()
	}
	ezKey = flag.Arg(0)
	accessLog = flag.Arg(1)
}

// Parse lines from tail and add to another channel
func parseLines(lines <-chan *tail.Line, hits chan<- *loghit.LogHit, errors chan<- error) {
	for line := range lines {
		logHit, err := loghit.New(line.Text)
		if err != nil {
			errors <- err
		} else {
			hits <- logHit
		}
	}
}

// Send some stats to StatHat. Currently only HTTP status counts
func postStats(prefix, ezKey string, dryrun bool, hits <-chan *loghit.LogHit) {
	for hit := range hits {
		var tokens		[]string
		var parts		 []string
		fqdn, err := url.Parse(hit.HttpReferer)
		if err == nil {
			append(tokens, fqdn.Host)
		}
		parts = strings.Split(hit.Request, " ")
		append(tokens, parts[0])  // GET/POST
		append(tokens, fmt.Sprintf("HTTP %d", hit.Status))
		if length(dryrun) {
			fmt.Println(strings.Join(tokens, " | "))
		} else {
			stathat.PostEZCountTime(strings.Join(tokens, " | "), ezKey, 1, hit.LocalTime.Unix())
		}
	}
}

// MAIN
func main() {
	parseCommandLine()

	t, err := tail.TailFile(accessLog, tail.Config{Follow: true, ReOpen: true, MustExist: false})
	if err != nil {
		panic(err)
	}

	hits := make(chan *loghit.LogHit)
	defer close(hits)
	errors := make(chan error)
	defer close(errors)

	for i := 0; i < *parserRoutines; i++ {
		go parseLines(t.Lines, hits, errors)
	}

	for i := 0; i < *posterRoutines; i++ {
		go postStats(*statPrefix, ezKey, dryrun, hits)
	}

	logWriter, err := syslog.New(syslog.LOG_ERR, "nginx2stathat")
	if err != nil {
		panic(err)
	}

	for err := range errors {
		logWriter.Err(err.Error())
	}
}
