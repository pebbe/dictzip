/*
A Go reader and writer for files in the random access `dictzip` format.
*/
package dictzip

//. Imports

import (
	"bytes"
	"compress/flate"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

//. Writer

/*
Levels range from 1 (BestSpeed) to 9 (BestCompression), Level 0 (NoCompression), -1 (DefaultCompression)
*/
func Write(r io.Reader, filename string, level int) error {

	const blocksize = 58315

	crc := crc32.NewIEEE()
	isize := 0

	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, level)
	if err != nil {
		return err
	}
	sizes := make([]int, 0)
	b := make([]byte, blocksize)
	total := 0
	eof := false
	for !eof {
		n, err := readfull(r, b)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				eof = true
			}
		}
		if n > 0 {
			crc.Write(b[:n])
			isize += n

			fw.Write(b[:n])
			fw.Flush()
			fw.Reset(&buf)

			l := buf.Len()
			sizes = append(sizes, l-total)
			total = l
		}
	}
	fw.Close()

	fp, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fp.Close()

	xfl := byte(0)
	if level == flate.BestCompression {
		xfl = 2
	} else if level == flate.BestSpeed {
		xfl = 4
	}
	now := time.Now().Unix()
	_, err = fp.Write([]byte{
		31, 139, 8, 4,
		byte(now & 255), byte((now >> 8) & 255), byte((now >> 16) & 255), byte((now >> 24) & 255),
		xfl, 255})
	if err != nil {
		return err
	}

	xlen := 10 + 2*len(sizes)
	ln := 6 + 2*len(sizes)
	_, err = fp.Write([]byte{
		byte(xlen & 255), byte((xlen >> 8) & 255),
		'R', 'A', byte(ln & 255), byte((ln >> 8) & 255),
		1, 0,
		byte(blocksize & 255), byte((blocksize >> 8) & 255),
		byte(len(sizes) & 255), byte((len(sizes) >> 8) & 255)})
	if err != nil {
		return err
	}
	for _, o := range sizes {
		_, err = fp.Write([]byte{byte(o & 255), byte((o >> 8) & 255)})
		if err != nil {
			return err
		}
	}

	_, err = fp.Write(buf.Bytes())
	if err != nil {
		return err
	}

	c := crc.Sum32()
	_, err = fp.Write([]byte{
		byte(c & 255), byte((c >> 8) & 255), byte((c >> 16) & 255), byte((c >> 24) & 255),
		byte(isize & 255), byte((isize >> 8) & 255), byte((isize >> 16) & 255), byte((isize >> 24) & 255),
	})
	if err != nil {
		return err
	}

	return nil

}

//. Reader

type Reader struct {
	fp        io.ReadSeeker
	offsets   []int64
	blocksize int64
	lock      sync.Mutex
}

func NewReader(rs io.ReadSeeker) (*Reader, error) {

	dz := &Reader{fp: rs}

	_, err := dz.fp.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	metadata := []byte{}

	p := 0

	h := make([]byte, 10)
	n, err := readfull(dz.fp, h)
	if err != nil {
		return nil, err
	}
	p += n

	if h[0] != 31 || h[1] != 139 {
		return nil, fmt.Errorf("Invalid header: %02X %02X\n", h[0], h[1])
	}

	if h[2] != 8 {
		return nil, fmt.Errorf("Unknown compression method:", h[2])
	}

	flg := h[3]

	if flg&4 != 0 {
		h := make([]byte, 2)
		n, err := readfull(dz.fp, h)
		if err != nil {
			return nil, err
		}
		p += n

		xlen := int(h[0]) + 256*int(h[1])
		h = make([]byte, xlen)
		n, err = readfull(dz.fp, h)
		if err != nil {
			return nil, err
		}
		p += n

		for q := 0; q < len(h); {
			si1 := h[q]
			si2 := h[q+1]
			ln := int(h[q+2]) + 256*int(h[q+3])

			if si1 == 'R' && si2 == 'A' {
				metadata = h[q+4 : q+4+ln]
			}

			q += 4 + ln
		}

	}

	// skip file name (8), file comment (16)
	for _, f := range []byte{8, 16} {
		if flg&f != 0 {
			h := make([]byte, 1)
			for {
				n, err := readfull(dz.fp, h)
				if err != nil {
					return nil, err
				}
				p += n
				if h[0] == 0 {
					break
				}
			}
		}
	}

	if flg&2 != 0 {
		h := make([]byte, 2)
		n, err := readfull(dz.fp, h)
		if err != nil {
			return nil, err
		}
		p += n
	}

	if len(metadata) < 6 {
		return nil, fmt.Errorf("Missing dictzip metadata")
	}

	version := int(metadata[0]) + 256*int(metadata[1])

	if version != 1 {
		return nil, fmt.Errorf("Unknown dictzip version:", version)
	}

	dz.blocksize = int64(metadata[2]) + 256*int64(metadata[3])
	blockcnt := int(metadata[4]) + 256*int(metadata[5])

	dz.offsets = make([]int64, blockcnt+1)
	dz.offsets[0] = int64(p)
	for i := 0; i < blockcnt; i++ {
		dz.offsets[i+1] = dz.offsets[i] + int64(metadata[6+2*i]) + 256*int64(metadata[7+2*i])
	}

	return dz, nil

}

func (dz *Reader) Get(start, size int64) ([]byte, error) {

	dz.lock.Lock()
	defer dz.lock.Unlock()

	start1 := dz.blocksize * (start / dz.blocksize)
	size1 := size + (start - start1)

	_, err := dz.fp.Seek(dz.offsets[start/dz.blocksize], 0)
	if err != nil {
		return nil, err
	}
	rd := flate.NewReader(dz.fp)

	data := make([]byte, size1)
	_, err = readfull(rd, data)
	if err != nil {
		return nil, err
	}

	return data[start-start1:], nil
}

// Using start and size in base64 notation, such as used by the dictunzip program.
func (dz *Reader) GetB64(start, size string) ([]byte, error) {
	start2, err := decode(start)
	if err != nil {
		return nil, err
	}
	size2, err := decode(size)
	if err != nil {
		return nil, err
	}
	return dz.Get(start2, size2)
}

//. Helper function

func readfull(fp io.Reader, buf []byte) (int, error) {
	ln := len(buf)
	for p := 0; p < ln; {
		n, err := fp.Read(buf[p:])
		p += n
		if err != nil {
			if err != io.EOF || p < ln {
				return p, err
			} else {
				return p, nil
			}
		}
	}
	return ln, nil
}

//. Base64 decoder

var (
	list = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

	index = []uint64{
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 62, 99, 99, 99, 63,
		52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 99, 99, 99, 99, 99, 99,
		99, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
		15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 99, 99, 99, 99, 99,
		99, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
	}
)

func decode(val string) (int64, error) {
	var result uint64
	var offset uint64

	for i := len(val) - 1; i >= 0; i-- {
		tmp := index[val[i]]
		if tmp == 99 {
			return 0, fmt.Errorf("Illegal character in base64 value: %v", val[i:i+1])
		}

		if (tmp<<offset)>>offset != tmp {
			return 0, fmt.Errorf("Type uint64 cannot store decoded base64 value: %v", val)
		}

		result |= tmp << offset
		offset += 6
	}
	return int64(result), nil
}
