package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"golang.org/x/sync/errgroup"
)

// CheckSignatures checks sha1 signatures for files in a directory concurrently
// and returns a error if a signature for a given file does not match.
// There should be a "sha1sum.txt" file in the directory with the format:
// 0c4ccc63a912bbd6d45174251415c089522e5c0e75286794ab1f86cb8e2561fd  taxi-01.csv
// f427b5880e9164ec1e6cda53aa4b2d1f1e470da973e5b51748c806ea5c57cbdf  taxi-02.csv
func CheckSignatures(rootDir string) error {
	file, err := os.Open(path.Join(rootDir, "sha1sum.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	sigs, err := parseSigFile(file)
	if err != nil {
		return err
	}

	var g errgroup.Group
	for name, signature := range sigs {
		fileName := path.Join(rootDir, name)
		expected := signature
		g.Go(func() error {
			sig, err := fileSig(fileName)
			if err != nil {
				return err
			}
			if sig != expected {
				return fmt.Errorf("%q - mismatch", fileName)
			}
			return nil
		})
	}

	return g.Wait()
}

// fileSig returns the fileName sha1 digital signature of the specified file.
func fileSig(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha1.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// parseSigFile parses the signature file and returns a map of path->signature.
func parseSigFile(r io.Reader) (map[string]string, error) {
	sigs := make(map[string]string)
	scanner := bufio.NewScanner(r)
	lnum := 0

	for scanner.Scan() {
		lnum++

		// Line example: 6c6427da7893932731901035edbb9214 nasa-00.log
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			return nil, fmt.Errorf("%d: bad line: %q", lnum, scanner.Text())
		}
		sigs[fields[1]] = fields[0]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return sigs, nil
}
