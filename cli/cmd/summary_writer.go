package cmd

import (
	"strings"
	"sync"
)

type summaryMessageWriter struct {
	mu      sync.Mutex
	sink    func(string)
	buffer  []string
	partial string
}

func newSummaryMessageWriter() *summaryMessageWriter {
	return &summaryMessageWriter{}
}

func (w *summaryMessageWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	data := w.partial + string(p)
	lines := strings.Split(data, "\n")
	w.partial = lines[len(lines)-1]
	emit := w.sink
	if emit == nil {
		w.buffer = append(w.buffer, lines[:len(lines)-1]...)
		w.mu.Unlock()
		return len(p), nil
	}
	w.mu.Unlock()

	for _, line := range lines[:len(lines)-1] {
		w.emitLine(emit, line)
	}
	return len(p), nil
}

func (w *summaryMessageWriter) emitLine(emit func(string), line string) {
	trimmed := strings.TrimRight(line, "\r")
	if strings.TrimSpace(trimmed) == "" {
		return
	}
	emit(trimmed)
}

func (w *summaryMessageWriter) SetSink(sink func(string)) {
	if sink == nil {
		return
	}

	w.mu.Lock()
	buffer := append([]string(nil), w.buffer...)
	partial := w.partial
	w.buffer = nil
	w.partial = partial
	w.sink = sink
	w.mu.Unlock()

	go func(lines []string, tail string) {
		for _, line := range lines {
			w.emitLine(sink, line)
		}
		if strings.TrimSpace(tail) != "" {
			w.emitLine(sink, tail)
		}
	}(buffer, partial)
}

func (w *summaryMessageWriter) Flush() {
	w.mu.Lock()
	sink := w.sink
	partial := w.partial
	w.partial = ""
	w.mu.Unlock()
	if sink != nil && strings.TrimSpace(partial) != "" {
		w.emitLine(sink, partial)
	}
}
