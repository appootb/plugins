// Package console implements logger.Logger as JSON lines on stdout/stderr.
//
// Blank-import registers the implementor. Messages at Error and above go to
// stderr; lower levels go to stdout. Structured fields from logger.Content are
// normalized (PATH, REQUEST, UID, …) for access-log style output.
package console

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/appootb/substratum/v2/logger"
	"github.com/appootb/substratum/v2/proto/go/common"
	"github.com/appootb/substratum/v2/proto/go/secret"
)

func init() {
	logger.RegisterImplementor(&stdJSON{
		out: os.Stdout,
		err: os.Stderr,
	})
}

type stdJSON struct {
	level int32
	// out/err are overridable for tests (default stdout/stderr).
	out io.Writer
	err io.Writer
}

// UpdateLevel sets the minimum level that will be emitted.
func (l *stdJSON) UpdateLevel(level logger.Level) {
	atomic.StoreInt32(&l.level, int32(level))
}

// Log writes one JSON object line if level is enabled.
func (l *stdJSON) Log(level logger.Level, md *common.Metadata, msg string, c logger.Content) {
	if int32(level) < atomic.LoadInt32(&l.level) {
		return
	}
	writer := l.out
	if writer == nil {
		writer = os.Stdout
	}
	if level >= logger.ErrorLevel {
		writer = l.err
		if writer == nil {
			writer = os.Stderr
		}
	}

	var outer logger.Content
	if c != nil {
		outer = make(logger.Content, len(c)+3)
	} else {
		outer = make(logger.Content, 3)
	}
	outer["LEVEL"] = level.String()
	outer["MESSAGE"] = msg
	outer["METADATA"] = md
	if c != nil {
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
				// Prefer account id; fall back to raw value if type mismatches.
				if info, ok := v.(*secret.Info); ok {
					outer["UID"] = info.GetAccount()
				} else {
					outer["UID"] = v
				}
			case logger.LogError:
				outer["ERROR"] = v
			default:
				outer[k] = v
			}
		}
	}

	content, err := json.Marshal(outer)
	if err != nil {
		_, _ = fmt.Fprintln(writer, "std_json marshal failed:", err.Error())
		return
	}
	_, _ = writer.Write(append(content, '\n'))
}
