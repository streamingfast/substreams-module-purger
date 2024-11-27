package datastore

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type FileType int

const (
	Output FileType = iota
	State
	Index
)

func FileInfo(in string) (uint64, uint64, FileType, error) {
	base := strings.TrimSuffix(path.Base(in), ".zst")
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return 0, 0, 0, fmt.Errorf("invalid file name: %q", in)
	}
	numbers := strings.Split(parts[0], "-")
	if len(numbers) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid file name: %q", in)
	}

	firstNum, err := strconv.ParseUint(numbers[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parsing first number: %w", err)
	}
	secondNum, err := strconv.ParseUint(numbers[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parsing second number: %w", err)
	}

	switch parts[1] {
	case "output":
		return firstNum, secondNum, Output, nil
	case "kv":
		return secondNum, firstNum, State, nil
	case "index":
		return firstNum, secondNum, Index, nil
	}

	return 0, 0, 0, fmt.Errorf("invalid file type: %q", parts[1])
}
