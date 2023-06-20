package godot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/francoispqt/gojay"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

var queues = []Queue{
	{Name: "Work1", Weight: 3},
	{Name: "Work2", Weight: 2},
	{Name: "Work3", Weight: 1},
	{Name: "default", Weight: 1},
}

type testDoter struct {
	Doter
	Name string
}

func NewTestJob(name string) *testDoter {
	d := testDoter{
		Doter: Doter{Queue: "Work1", Retry: false, RetryCount: 5},
		Name:  name,
	}
	return &d
}

func (d testDoter) Run(args ...interface{}) error {
	fmt.Println("testJob", args)
	time.Sleep(time.Second)
	return fmt.Errorf("xxx retryJob")
}

func TestDot(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	pong, err := client.Ping(ctx).Result()
	fmt.Println(pong, err)

	t.Run("NewGoDot", func(t *testing.T) {
		godot := NewGoDot(ctx, client, queues, 10)

		gdc := NewGoDotCli(client)
		//d := NewTestJob(fmt.Sprintf("Job:%d", 1))
		//a := time.Now().Add(10 * time.Second).Unix()
		for i := 0; i < 100; i++ {
			//fmt.Println(d)
			gdc.RunAt(ctx, 1000, DefaultDoter, "test_at")
			//godot.Run(d, "test", i)
			gdc.Run(ctx, DefaultDoter, "test_at")
			//if want, got := true, d.Execed; want == got {
			//	t.Errorf("want %t got %t", want, got)
			//}
		}
		defer godot.WaitJob()

	})

}

var (
	genID = []struct {
		name string
		fun  func() string
	}{
		{"cryptoJid", generateJid},
		{"googleUid", googleJid},
		{"googleUid", googleJidV2},
	}
)

func BenchmarkGenID(b *testing.B) {
	fmt.Println(googleJid())
	for _, f := range genID {
		b.Run(f.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.fun()
			}
		})
	}
}

var (
	encodeJson = []struct {
		name string
		fun  func(string)
	}{
		{"decode", decodeJson},
		{"unmarshal", unmarshalJson},
		{"decode iter", decodeJsoniter},
		{"unmarshal iter", unmarshalJsoniter},
		{"unmarshal map", unmarshalJsonMap},
		{"decode map", decodeJsonMap},
	}
)

var jsonBlob = `[
	{"Name": "Platypus", "Order": "Monotremata"},
	{"Name": "Quoll",    "Order": "Dasyuromorphia"}
]`
var jsonOne = `
	{"Name": "Quoll",    "Order": "Dasyuromorphia"}
`

type Animal struct {
	Name  string
	Order string
}

var animals Animal

func BenchmarkDecodeJSON(b *testing.B) {

	fmt.Println(googleJid())
	for _, f := range encodeJson {
		b.Run(f.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.fun(jsonOne)
			}
		})
	}
}

func unmarshalJson(data string) {
	err := json.Unmarshal([]byte(data), &animals)
	if err != nil {
		fmt.Println("retryJob:", err)
	}
}

func decodeJson(data string) {
	dec := json.NewDecoder(strings.NewReader(data))
	dec.Decode(&animals)

}
func unmarshalJsoniter(data string) {
	err := jsoniter.Unmarshal([]byte(data), &animals)
	if err != nil {
		fmt.Println("retryJob:", err)
	}
}

func decodeJsoniter(data string) {
	dec := jsoniter.NewDecoder(strings.NewReader(data))
	dec.Decode(&animals)

}

func unmarshalJsonJay(data string) {
	err := gojay.Unmarshal([]byte(data), &animals)
	if err != nil {
		fmt.Println("retryJob:", err)
	}
}

func decodeJsonJay(data string) {
	dec := gojay.NewDecoder(strings.NewReader(data))
	dec.Decode(&animals)
}

func unmarshalJsonMap(data string) {
	var amap map[string]interface{}
	err := jsoniter.Unmarshal([]byte(data), &amap)
	if err != nil {
		fmt.Println("retryJob:", err)
	}
}

func decodeJsonMap(data string) {
	var amap map[string]interface{}
	dec := jsoniter.NewDecoder(strings.NewReader(data))
	err := dec.Decode(&amap)
	if err != nil {
		fmt.Println("retryJob:", err)
	}
}
