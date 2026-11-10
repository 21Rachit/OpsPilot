package notifier

import (
	"fmt"
	"os"
	"path/filepath"
)

// FileWriter appends one line per event to <dir>/consumer_<id>_hello.txt.
type FileWriter struct {
	path string
}

func NewFileWriter(dir, consumerID string) (*FileWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	return &FileWriter{path: filepath.Join(dir, "consumer_"+consumerID+"_hello.txt")}, nil
}

// Path is the file this writer appends to.
func (w *FileWriter) Path() string {
	return w.path
}

// Write appends one line. The file is opened on every write, so you can delete or
// chmod it while the consumer is running.
func (w *FileWriter) Write(line string) error {
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("append line: %w", err) // e.g. "open <path>: permission denied": may be fixed, so retry
	}
	if _, err := fmt.Fprintln(f, line); err != nil {
		f.Close()
		return fmt.Errorf("append line: %w", err)
	}
	return f.Close()
}
