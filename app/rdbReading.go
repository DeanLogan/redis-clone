package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"
)

// readEncodedInt reads an encoded integer from the provided reader.
// It returns the decoded integer and any error encountered.
func readEncodedInt(reader *bufio.Reader) (int, error) {
	mask := byte(0b11000000)
	// Read the first byte
	b0, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	if b0&mask == 0b00000000 { // If the two most significant bits are 00, the value is in the first byte
		return int(b0), nil
	} else if b0&mask == 0b01000000 { // If the two most significant bits are 01, the value is in the first and second byte
		b1, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(b1)<<6 | int(b0&mask), nil
	} else if b0&mask == 0b10000000 { // If the two most significant bits are 10, the value is in the first four bytes
		b1, _ := reader.ReadByte()
		b2, _ := reader.ReadByte()
		b3, _ := reader.ReadByte()
		b4, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		
		return int(b1)<<24 | int(b2)<<16 | int(b3)<<8 | int(b4), nil
	} else if b0 >= 0b11000000 && b0 <= 0b11000010 { // Special format: Integers as String
		var b1, b2, b3, b4 byte
		b1, err = reader.ReadByte()
		if b0 >= 0b11000001 {
			b2, err = reader.ReadByte()
		}
		if b0 == 0b11000010 {
			b3, _ = reader.ReadByte()
			b4, err = reader.ReadByte()
		}
		if err != nil {
			return 0, err
		}
		return int(b1) | int(b2)<<8 | int(b3)<<16 | int(b4)<<24, nil
	} else {
		return 0, errors.New("not implemented")
	}
}

// readEncodedString reads an encoded string from the provided reader.
// It returns the decoded string and any error encountered.
func readEncodedString(reader *bufio.Reader) (string, error) {
	size, err := readEncodedInt(reader)
	if err != nil {
		return "", err
	}
	data := make([]byte, size)
	actual, err := reader.Read(data)
	if err != nil {
		return "", err
	}
	if int(size) != actual {
		return "", errors.New("unexpected string length")
	}
	return string(data), nil
}

// readRDB reads a Redis RDB file from the provided path.
// It returns any error encountered during the reading process.
func readRDB(rdbPath string) error {
	file, err := os.Open(rdbPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	header := make([]byte, 9)
	reader.Read(header)
	if slices.Compare(header[:5], []byte("REDIS")) != 0 {
		return errors.New("not a RDB file")
	}

	version, _ := strconv.Atoi(string(header[5:]))
	fmt.Printf("File version: %d\n", version)

	for eof := false; !eof; {

		startDataRead := false
		opCode, err := reader.ReadByte()

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch opCode {
			case 0xFA: // Auxiliary fields
				key, err := readEncodedString(reader)
				if err != nil {
					fmt.Printf("Error reading key: %v\n", err)
					return nil
				}
				switch key {
				case "redis-ver":
					value, err := readEncodedString(reader)
					if err != nil {
						fmt.Printf("Error reading value: %v\n", err)
						return nil
					}
					fmt.Printf("Aux: %s = %v\n", key, value)
				case "redis-bits":
					bits, err := readEncodedInt(reader)
					if err != nil {
						fmt.Printf("Error reading bits: %v\n", err)
						return nil
					}
					fmt.Printf("Aux: %s = %v\n", key, bits)
				case "ctime":
					ctime, err := readEncodedInt(reader)
					if err != nil {
						fmt.Printf("Error reading ctime: %v\n", err)
						return nil
					}
					fmt.Printf("Aux: %s = %v\n", key, ctime)
				case "used-mem":
					usedmem, err := readEncodedInt(reader)
					if err != nil {
						fmt.Printf("Error reading usedmem: %v\n", err)
						return nil
					}
					fmt.Printf("Aux: %s = %v\n", key, usedmem)
				case "aof-preamble":
					size, err := readEncodedInt(reader)
					if err != nil {
						fmt.Printf("Error reading size: %v\n", err)
						return nil
					}
					fmt.Printf("Aux: %s = %d\n", key, size)
				default:
					fmt.Printf("Unknown auxiliary field: %q\n", key)
			}
			case 0xFB: // Hash table sizes for the main keyspace and expires
				keyspace, err := readEncodedInt(reader)
				if err != nil {
					fmt.Printf("Error reading keyspace: %v\n", err)
					return nil
				}
				expires, err := readEncodedInt(reader)
				if err != nil {
					fmt.Printf("Error reading expires: %v\n", err)
					return nil
				}
				fmt.Printf("Hash table sizes: keyspace = %d, expires = %d\n", keyspace, expires)
				startDataRead = true
			
			case 0xFE: // Database Selector
				db, err := readEncodedInt(reader)
				if err != nil {
					fmt.Printf("Error reading db: %v\n", err)
					return nil
				}
				fmt.Printf("Database Selector = %d\n", db)
			
			case 0xFF: // End of the RDB file
				eof = true
			default:
				fmt.Printf("Unknown op code: %d\n", opCode)
		}

		if startDataRead {
			for {
				valueType, err := reader.ReadByte()
				if err != nil {
					return err
				}

				var expiration time.Time
				if valueType == 0xFD {
					bytes := make([]byte, 4)
					reader.Read(bytes)
					expiration = time.Unix(int64(bytes[0])|int64(bytes[1])<<8|int64(bytes[2])<<16|int64(bytes[3])<<24, 0)
					valueType, err = reader.ReadByte()
				} else if valueType == 0xFC {
					bytes := make([]byte, 8)
					reader.Read(bytes)
					expiration = time.UnixMilli(int64(bytes[0]) | int64(bytes[1])<<8 | int64(bytes[2])<<16 | int64(bytes[3])<<24 |
						int64(bytes[4])<<32 | int64(bytes[5])<<40 | int64(bytes[6])<<48 | int64(bytes[7])<<56)
					valueType, err = reader.ReadByte()
				}

				if err != nil {
					return err
				}

				if valueType > 14 {
					startDataRead = false
					reader.UnreadByte()
					break
				}

				key, _ := readEncodedString(reader)
				value, _ := readEncodedString(reader)
				fmt.Printf("Reading key/value: %q => %q Expiration: (%v)\n", key, value, expiration)

				now := time.Now()

				if expiration.IsZero() || expiration.After(now) {
					if expiration.After(now) {
						ttl[key] = expiration
					}
					store[key] = value
				}
			}
		}
	}

	return nil
}