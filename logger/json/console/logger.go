package console

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/appootb/substratum/logger"
	"github.com/appootb/substratum/proto/go/common"
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
	//
	writer := os.Stdout
	if level >= logger.ErrorLevel {
		writer = os.Stderr
	}
	//
	if c == nil {
		c = make(logger.Content, 3)
	}
	c["_MSG_.message"] = msg
	c["_MSG_.metadata"] = md
	c["_MSG_.level"] = level.String()
	//
	content, err := json.Marshal(c)
	if err != nil {
		_, _ = fmt.Fprintln(writer, "std_json marshal failed:", err.Error())
	} else {
		_, _ = writer.WriteString(string(content) + "\n")
	}
}
