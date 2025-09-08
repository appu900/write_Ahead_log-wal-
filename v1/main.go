// this code is resposible for write entries as []byte sequentially to a file
// and reades back then

package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

type WAL struct {
	file *os.File
}

// this function is responsible for create a WAL file
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: f,
	}, nil
}

// function for append entries
func (w *WAL) Append(data []byte) error {
	length := uint32(len(data))
	// write length
	if err := binary.Write(w.file, binary.BigEndian, length); err != nil {
		return err
	}
	// write data
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

// ** Read all entirs back
func (w *WAL) ReadAll() ([][]byte, error) {
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(w.file)
	var entries [][]byte
	for {
		var length uint32
		err := binary.Read(reader, binary.BigEndian, &length)

		// this indicated if reading failes or end_of_file happen
		if err != nil {
			break
		}
		buf := make([]byte, length)
		_, err = reader.Read(buf)
		if err != nil {
			break
		}
		entries = append(entries, buf)
	}
	return entries, nil
}

func main() {
	wal, _ := NewWAL("wal.log")
	wal.Append([]byte("SET x=10"))
	wal.Append([]byte("SET y=20"))

	entries, _ := wal.ReadAll()
	for _, e := range entries {
		fmt.Println("Recovered", string(e))
	}
}
