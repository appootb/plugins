package console

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/appootb/substratum/v2/logger"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
)

func init() {
	logger.RegisterImplementor(&stdJSON{})
}

type stdJSON struct {
	level int32
}

func (l *stdJSON) UpdateLevel(level logger.Level) {
	atomic.StoreInt32(&l.level, int32(level))
}

func (l *stdJSON) Log(level logger.Level, md *common.Metadata, msg string, c logger.Content) {
	if int32(level) < atomic.LoadInt32(&l.level) {
		return
	}
	writer := os.Stdout
	if level >= logger.ErrorLevel {
		writer = os.Stderr
	}
	//
	var outer logger.Content
	if c != nil {
		outer = make(logger.Content, len(c)+3)
	} else {
		outer = make(logger.Content, 3)
	}
	outer["LEVEL"] = level.String()
	outer["MESSAGE"] = msg
	outer["METADATA"] = md
	if c == nil {
		goto Print
	}
	//
	for k, v := range c {
		switch k {
		case logger.LogConsumed:
			outer["CONSUMED"] = v
		case logger.LogPath:
			outer["PATH"] = v
		case logger.LogRequest:
			outer["REQUEST"] = v
		case logger.LogResponse:
			outer["RESPONSE"] = v
		case logger.LogSecret:
			outer["UID"] = v.(*secret.Info).GetAccount()
		case logger.LogError:
			outer["ERROR"] = v
		default:
			outer[k] = v
		}
	}

Print:
	content, err := json.Marshal(outer)
	if err != nil {
		_, _ = fmt.Fprintln(writer, "std_json marshal failed:", err.Error())
	} else {
		_, _ = writer.WriteString(string(content) + "\n")
	}
}
