package streamdiag

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultDir         = "/var/log/deepseek-proxy/stream-diagnostics"
	QueueSize          = 256
	MaxBytes     int64 = 50 << 20
	fileMaxBytes int64 = 1 << 20
	retention          = 7 * 24 * time.Hour
)

// Only this sidecar namespace is managed, independent of the proxy's 50 MiB.
var ownedName = regexp.MustCompile(`^sidecar-stream-[0-9]{8}-[0-9]{6}-[0-9]{6}\.jsonl$`)

type Stats struct {
	Dropped, FilesystemFailures, ParserFailures uint64
}

type Recorder struct {
	dir                                 string
	queue                               chan Metadata
	stop, done                          chan struct{}
	closed                              atomic.Bool
	once                                sync.Once
	enqueueMu                           sync.RWMutex
	dropped, fsFailures, parserFailures atomic.Uint64
	// Everything below is accessed only by the writer goroutine.
	file              *os.File
	path, day         string
	size              int64
	serial            int
	maxBytes, fileMax int64
	now               func() time.Time
}

// NewFromEnv does no filesystem work in the request path. Only literal "1" is on.
func NewFromEnv() *Recorder {
	if os.Getenv("WB2A_STREAM_DIAGNOSTICS") != "1" {
		return nil
	}
	return New(environmentDir())
}

func environmentDir() string {
	dir := os.Getenv("WB2A_STREAM_DIAGNOSTICS_DIR")
	if dir == "" {
		dir = DefaultDir
	}
	return dir
}

func newRecorder(dir string) *Recorder {
	return &Recorder{dir: dir, queue: make(chan Metadata, QueueSize), stop: make(chan struct{}), done: make(chan struct{}), maxBytes: MaxBytes, fileMax: fileMaxBytes, now: time.Now}
}

func New(dir string) *Recorder {
	r := newRecorder(dir)
	go r.run()
	return r
}

func (r *Recorder) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{r.dropped.Load(), r.fsFailures.Load(), r.parserFailures.Load()}
}

func (r *Recorder) enqueue(m Metadata) {
	r.enqueueMu.RLock()
	defer r.enqueueMu.RUnlock()
	if r.closed.Load() {
		r.dropped.Add(1)
		return
	}
	select {
	case r.queue <- m:
	default:
		r.dropped.Add(1)
	}
}

func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.enqueueMu.Lock()
		r.closed.Store(true)
		close(r.stop)
		r.enqueueMu.Unlock()
		<-r.done
	})
}

func (r *Recorder) run() {
	defer close(r.done)
	defer r.closeFile()
	// Startup and idle retention must work even if no eligible request arrives.
	r.maintain(0)
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case m := <-r.queue:
			r.write(m)
		case <-tick.C:
			r.maintain(0)
		case <-r.stop:
			for {
				select {
				case m := <-r.queue:
					r.write(m)
				default:
					return
				}
			}
		}
	}
}

func (r *Recorder) fail() { r.fsFailures.Add(1) }

func (r *Recorder) closeFile() {
	if r.file != nil {
		if r.file.Close() != nil {
			r.fail()
		}
	}
	r.file, r.path, r.day, r.size = nil, "", "", 0
}

func (r *Recorder) directory() bool {
	if os.MkdirAll(r.dir, 0700) != nil {
		r.fail()
		return false
	}
	st, err := os.Lstat(r.dir)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		r.fail()
		return false
	}
	if os.Chmod(r.dir, 0700) != nil {
		r.fail()
		return false
	}
	return true
}

type archive struct {
	path string
	size int64
	at   time.Time
}

// maintain reserves bytes before writes; unlike post-write pruning this never
// exceeds the cap. It fails closed for diagnostics, not for model requests.
func (r *Recorder) maintain(reserve int64) bool {
	if !r.directory() {
		return false
	}
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		r.fail()
		return false
	}
	now := r.now()
	var files []archive
	var total int64
	for _, e := range entries {
		if !ownedName.MatchString(e.Name()) {
			continue
		}
		st, err := e.Info()
		if err != nil || !st.Mode().IsRegular() {
			r.fail()
			return false
		}
		path := filepath.Join(r.dir, e.Name())
		if os.Chmod(path, 0600) != nil {
			r.fail()
			return false
		}
		if st.ModTime().Before(now.Add(-retention)) {
			if path == r.path {
				r.closeFile()
			}
			if os.Remove(path) != nil {
				r.fail()
				return false
			}
			continue
		}
		files = append(files, archive{path, st.Size(), st.ModTime()})
		total += st.Size()
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].at.Equal(files[j].at) {
			return files[i].path < files[j].path
		}
		return files[i].at.Before(files[j].at)
	})
	for _, f := range files {
		if total+reserve <= r.maxBytes {
			break
		}
		if f.path == r.path {
			r.closeFile()
		}
		if os.Remove(f.path) != nil {
			r.fail()
			return false
		}
		total -= f.size
	}
	return total+reserve <= r.maxBytes
}

func (r *Recorder) open(now time.Time) bool {
	for tries := 0; tries < 1000; tries++ {
		r.serial = (r.serial + 1) % 1000000
		path := filepath.Join(r.dir, fmt.Sprintf("sidecar-stream-%s-%06d.jsonl", now.UTC().Format("20060102-150405"), r.serial))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			r.fail()
			return false
		}
		r.file, r.path, r.day, r.size = f, path, now.UTC().Format("20060102"), 0
		return true
	}
	r.fail()
	return false
}

func (r *Recorder) write(m Metadata) {
	raw, err := encode(m)
	if err != nil {
		r.parserFailures.Add(1)
		r.dropped.Add(1)
		return
	}
	raw = append(raw, '\n')
	n := int64(len(raw))
	if n > r.fileMax || n > r.maxBytes {
		r.dropped.Add(1)
		return
	}
	now := r.now()
	if r.file != nil && (r.day != now.UTC().Format("20060102") || r.size+n > r.fileMax) {
		r.closeFile()
	}
	if !r.maintain(n) {
		r.dropped.Add(1)
		return
	}
	if r.file == nil && !r.open(now) {
		r.dropped.Add(1)
		return
	}
	written, err := r.file.Write(raw)
	r.size += int64(written)
	if err != nil || written != len(raw) {
		r.fail()
		r.dropped.Add(1)
		r.closeFile()
	}
}
