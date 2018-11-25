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
)

// map[string]interfaceをパースした際にjsonNumberをstringで受けたいのでjson.Numberを利用する
type Quote struct {
	Price            json.Number `json:"price"`
	Volume24h        json.Number `json:"volume_24h"`
	MarketCap        json.Number `json:"market_cap"`
	PercentChange1h  json.Number `json:"percent_change_1h"`
	PercentChange24h json.Number `json:"percent_change_24h"`
	PercentChange7d  json.Number `json:"percent_change_7d"`
}

type Quotes struct {
	JPY Quote `json:"JPY"`
	USD Quote `json:"USD"`
	BTC Quote `json:"BTC"`
}

type CoinMarketInfoResponse struct {
	ID              int32       `json:"id"`
	Name            string      `json:"name"`
	Symbol          string      `json:"symbol"`
	WebsiteSlug     string      `json:"slug"`
	Rank            int32       `json:"cmc_rank"`
	AvailableSupply json.Number `json:"circulating_supply"`
	TotalSupply     json.Number `json:"total_supply"`
	MaxSupply       json.Number `json:"max_supply"`
	Quotes          Quotes      `json:"quote"`
	LastUpdated     string      `json:"last_updated"`
}

func loadEnv(){
	err:= godotenv.Load("./.env")
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

func mainHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
	fmt.Printf("response returned\n")
}

func decodeBody(res *http.Response, out interface{}) error {
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	return decoder.Decode(out)
}

// GetCoinMarketInfos ...
func GetCoinMarketInfos() ([]CoinMarketInfoResponse, error) {
	url := "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=30"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	req.Header.Set("X-CMC_PRO_API_KEY", os.Getenv("APIKEY"))
	client := new(http.Client)
	resp, err := client.Do(req)
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

	var coinMarketInfos []CoinMarketInfoResponse
	err = utils.Parse(data, &coinMarketInfos)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return coinMarketInfos, nil
}

func processTick(client *firestore.Client, ctx context.Context) {
	infos, err := GetCoinMarketInfos()
	if err != nil {
		log.Printf("error: %+v", err)
		return
	}

	//batch := client.Batch()
	//for _, info := range infos {
	//	fmt.Printf("add coin: %s\n", info.Symbol)
	//	docRef := client.Doc(fmt.Sprintf("Market/%s", info.Symbol))
	//	batch.Set(docRef, map[string]interface{}{
	//		"Name": info.Name,
	//		"Rank" : info.Rank,
	//		"USD" : info.Quotes.USD.Price,
	//	})
	//}
	//batch.Commit(ctx)

	for _, info := range infos {
		fmt.Printf("add coin: %s\n", info.Symbol)
		client.Doc(fmt.Sprintf("Market/%s", info.Symbol)).Set(ctx, map[string]interface{}{
			"Name": info.Name,
			"Rank": info.Rank,
			"USD":  info.Quotes.USD.Price,
		})
	}

	fmt.Printf("market info is updated.\n")
}

func batchProcessTick(client *firestore.Client, ctx context.Context) {
	infos, err := GetCoinMarketInfos()
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
			"Rank" : info.Rank,
			"USD" : info.Quotes.USD.Price,
		})
	}
	batch.Commit(ctx)

	fmt.Printf("market info is updated.\n")
}

func main() {
	loadEnv()
	ctx := context.Background()
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: os.Getenv("PROJECT_ID")})
	if err != nil {
		log.Fatalf("failed to create new app")
	}
	client, err := app.Firestore(ctx)



	ticker := time.NewTicker(2 * time.Minute)
	ticker2 := time.NewTicker(3 * time.Minute)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("tick.\n")
				processTick(client, ctx)
			case <- ticker2.C:
				fmt.Println("tick2")
				batchProcessTick(client, ctx)
			}
		}
	}()
	fmt.Printf("market info updater is started.\n")

	fmt.Printf("main endpoint is added.\n")
	http.HandleFunc("/", mainHandler)
	log.Fatal(http.ListenAndServe(":10000", nil))
}
