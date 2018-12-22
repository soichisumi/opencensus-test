package main

import (
	"net/http"
	"fmt"
	"log"
	"cloud.google.com/go/firestore"
	"github.com/joho/godotenv"
	"os"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/GincoInc/go-global/utils"
	"firebase.google.com/go"
	"time"
	"context"
	"go.opencensus.io/trace"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/stats/view"
)

type CoinMarketInfo struct {
	ID              int32       `json:"id"`
	Name            string      `json:"name"`
	Symbol          string      `json:"symbol"`
	Rank            int32       `json:"cmc_rank"`
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	_, span := trace.StartSpan(context.Background(), "html response")
	defer span.End()

	fmt.Fprintf(w, "OK")
	fmt.Printf("response returned\n")
}

func loadEnv() {
	err := godotenv.Load("./.env")
	if err != nil {
		log.Fatalf("error loading .env file: : %+v", err)
	}
	if os.Getenv("APIKEY") == "" {
		log.Fatalf("env variable \"APIKEY\" is not specified.")
	}
	if os.Getenv("PROJECT_ID") == "" {
		log.Fatalf("env variable \"PROJECT_ID\" is not specified.")
	}
}

// GetCoinMarketInfos ...
func GetCoinMarketInfos(ctx context.Context) ([]CoinMarketInfo, error) {
	_, span := trace.StartSpan(ctx, "getCMCInfos")
	defer span.End()

	url := "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=10"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	req.Header.Set("X-CMC_PRO_API_KEY", os.Getenv("APIKEY"))

	client := &http.Client{Transport: &ochttp.Transport{Propagation: &tracecontext.HTTPFormat{}}}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var jsonMap map[string]interface{}
	if err = json.NewDecoder(resp.Body).Decode(&jsonMap); err != nil {
		return nil, errors.WithStack(err)
	}
	status, ok := jsonMap["status"].(map[string]interface{})
	if !ok {
		return nil, errors.New(fmt.Sprintf("failed to parsing status object. jsonMap: %+v", jsonMap))
	}

	if int(status["error_code"].(float64)) != 0 { // non-nil
		errMsg := status["error_message"].(string) // non-nil when errCode != 0
		return nil, errors.New(errMsg)
	}

	data, ok := jsonMap["data"]
	if !ok {
		return nil, errors.New(fmt.Sprintf("failed to parsing data object. jsonMap: %+v", jsonMap))
	}

	var coinMarketInfos []CoinMarketInfo
	err = utils.Parse(data, &coinMarketInfos)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return coinMarketInfos, nil
}

func processTick(client *firestore.Client) {
	ctx, span := trace.StartSpan(context.Background(), "processTick.naive")
	defer span.End()

	infos, err := GetCoinMarketInfos(ctx)
	if err != nil {
		log.Printf("error: %+v", err)
		return
	}

	for _, info := range infos {
		fmt.Printf("add coin: %s\n", info.Symbol)
		client.Doc(fmt.Sprintf("Market/%s", info.Symbol)).Set(ctx, map[string]interface{}{
			"Name": info.Name,
			"Rank": info.Rank,
			"Symbol":  info.Symbol,
		})
	}

	fmt.Printf("market info is updated.\n")
}

func batchProcessTick(client *firestore.Client) {
	ctx, span := trace.StartSpan(context.Background(), "processTick.batch")
	defer span.End()

	infos, err := GetCoinMarketInfos(ctx)
	if err != nil {
		log.Printf("error: %+v", err)
		return
	}

	batch := client.Batch()
	for _, info := range infos {
		fmt.Printf("add coin: %s\n", info.Symbol)
		docRef := client.Doc(fmt.Sprintf("BatchMarket/%s", info.Symbol))
		batch.Set(docRef, map[string]interface{}{
			"Name": info.Name,
			"Rank": info.Rank,
			"Symbol":  info.Symbol,
		})
	}
	batch.Commit(ctx)

	fmt.Printf("market info is updated.\n")
}

func main() {
	loadEnv()

	exporter, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID: os.Getenv("PROJECT_ID"),
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(0.5)})
	view.SetReportingPeriod(60 * time.Second)
	view.RegisterExporter(exporter)

	ctx := context.Background()
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: os.Getenv("PROJECT_ID")})
	if err != nil {
		log.Fatalf("failed to create new app")
	}
	client, err := app.Firestore(ctx)

	ticker1 := time.NewTicker(1 * time.Minute)
	ticker2 := time.NewTicker(1 * time.Minute)
	fmt.Printf("start market info updater.\n")
	for {
		select {
		case <-ticker1.C:
			fmt.Println("tick1")
			processTick(client)
		case <-ticker2.C:
			fmt.Println("tick2")
			batchProcessTick(client)
		}
	}
}
