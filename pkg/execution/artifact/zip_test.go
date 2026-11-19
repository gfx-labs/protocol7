package artifact

import (
	"archive/zip"
	"bytes"
)

// newZip makes a zip with entries named exactly as given, which zip.Writer
// does not refuse.
func newZip(buffer *bytes.Buffer, entries map[string]string) *zip.Reader {
	writer := zip.NewWriter(buffer)
	for name, data := range entries {
		out, _ := writer.Create(name)
		out.Write([]byte(data))
	}
	writer.Close()
	reader, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		panic(err)
	}
	return reader
}
