package godot

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func RandQueue(queueNames []string) []string {
	rand.Shuffle(len(queueNames), func(i, j int) {
		queueNames[i], queueNames[j] = queueNames[j], queueNames[i]
	})
	queue := Unique(queueNames)
	return queue
}

func Unique(in []string) []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range in {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func NowTimeStamp() string {
	now := fmt.Sprintf("%d", time.Now().Unix())
	return now
}

func getStructName(in interface{}) string {
	obj := reflect.Indirect(reflect.ValueOf(in))
	typ := obj.Type()
	jobName := typ.Name()
	return jobName
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(crand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func googleJid() string {
	id, err := uuid.NewUUID()
	if err != nil {
		// handle retryJob
	}
	return fmt.Sprintf(id.String())
}
func googleJidV2() string {
	id := uuid.New()
	return id.String()
}
