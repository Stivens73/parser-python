package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"encoding/csv"
	"os"
	"io"
	"sync"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	"fmt"
	"gopkg.in/square/go-jose.v1/json"
)

const (
	PATH_CSV = "prepared.csv"
	_ZONE = "us-west-1"
	_ID = "id"
	_KEY = "key"
)

var _BUCKET = "go-test-buck"

var uploader *s3manager.Uploader

var amount = 197122
var count_ int64 = 0
var co = &count_


var Cred struct{
	ID, KEY string
}

func init() {
	f, err := os.Open("cred.json")
	if err != nil {
		panic(err)
	}
	err = json.NewDecoder(f).Decode(&Cred)
	if err != nil {
		panic(err)
	}
	uploader = s3manager.NewUploader(session.New(&aws.Config{
		Region: aws.String(_ZONE),
		Credentials: credentials.NewStaticCredentials(Cred.ID, Cred.KEY, ""),
	}))
}

func main() {
	count := 0
	go func() {
		for {
			time.Sleep(time.Second)
			am := atomic.LoadInt64(co)
			fmt.Printf("%f percents\n", 100 * float64(am) / float64(amount))
		}
	}()

	file, err := os.Open(PATH_CSV)
	if err != nil {
		panic(err)
	}
	csv_reader := csv.NewReader(file)

	// sync
	wg := sync.WaitGroup{}
	limit_ := 100
	limit := make(chan int, limit_)
	for i := 0; i < limit_ - 1; i++ {
		limit <- 1
	}

	for {
		<-limit
		record, err := csv_reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			println(err.Error())
			continue
		}
		wg.Add(1)
		count++
		go func(record []string) {
			defer func() {
				wg.Done()
				limit <- 1
				atomic.AddInt64(co, 1)
			}()
			name := strings.Replace(record[2], " ", "_", -1) + "/" + record[0]
			url := record[1]
			resp, err := http.Get(url)
			// defer resp.Body.Close()
			if err != nil {
				println(err.Error())
				return
			}
			// smock flickr image
			if resp.ContentLength < 1024 * 15 {
				println("Image was deleted")
				return
			}
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: &_BUCKET,
				Key: &name,
				Body: resp.Body,
			})
			if err != nil {
				println(err.Error())
			}
			return
		}(record)
	}
	wg.Wait()
}