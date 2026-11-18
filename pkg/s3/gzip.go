package s3

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(io.Discard) },
}

// GzipBytes compresses in using a pooled gzip writer.
func GzipBytes(in []byte) ([]byte, error) {
	w := gzipWriterPool.Get().(*gzip.Writer)
	defer func() {
		w.Reset(io.Discard)
		gzipWriterPool.Put(w)
	}()

	var b bytes.Buffer
	w.Reset(&b)
	if _, err := w.Write(in); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
