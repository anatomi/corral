package corral

import (
	"bufio"

	"github.com/bcongdon/corral/internal/pkg/backend"
)

// inputSplit contains the information about a contiguous chunk of an input file.
// startOffset and endOffset are inclusive. For example, if the startOffset was 10
// and the endOffset was 14, then the inputSplit would describe a 5 byte chunk
// of the file.
type inputSplit struct {
	filename    string // The file that the input split operates on
	startOffset int64  // The starting byte index of the split in the file
	endOffset   int64  // The ending byte index (inclusive) of the split in the file
}

// Size returns the number of bytes that the inputSplit spans
func (i inputSplit) Size() int64 {
	return i.endOffset - i.startOffset + 1
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func splitInputFile(file backend.FileInfo, maxSplitSize int64) []inputSplit {
	splits := make([]inputSplit, 0)

	for startOffset := int64(0); startOffset < file.Size; startOffset += maxSplitSize {
		endOffset := min(startOffset+maxSplitSize-1, file.Size-1)
		newSplit := inputSplit{
			filename:    file.Name,
			startOffset: startOffset,
			endOffset:   endOffset,
		}
		splits = append(splits, newSplit)
	}

	return splits
}

// inputBin is a collection of inputSplits.
type inputBin struct {
	splits []inputSplit
	// The total size of the inputBin. (The sum of the size of all splits)
	size int64
}

// packInputSplits partitions inputSplits into bins.
// The combined size of each bin will be no greater than maxBinSize
func packInputSplits(splits []inputSplit, maxBinSize int64) [][]inputSplit {
	if len(splits) == 0 {
		return [][]inputSplit{}
	}

	bins := make([]*inputBin, 1)
	bins[0] = &inputBin{
		splits: make([]inputSplit, 0),
		size:   0,
	}

	// Partition splits into bins using a naive Next-Fit packing algorithm
	for _, split := range splits {
		currBin := bins[len(bins)-1]

		if currBin.size+split.Size() <= maxBinSize {
			currBin.splits = append(currBin.splits, split)
			currBin.size += split.Size()
		} else {
			newBin := &inputBin{
				splits: []inputSplit{split},
				size:   split.Size(),
			}
			bins = append(bins, newBin)
		}
	}

	binnedSplits := make([][]inputSplit, len(bins))
	for i, bin := range bins {
		binnedSplits[i] = bin.splits
	}
	return binnedSplits
}

// countingSplitFunc wraps a bufio.SplitFunc and keeps track of the number of bytes advanced.
// Upon each scan, the value of *bytesRead will be incremented by the number of bytes
// that the SplitFunc advances.
func countingSplitFunc(split bufio.SplitFunc, bytesRead *int64) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		adv, tok, err := split(data, atEOF)
		(*bytesRead) += int64(adv)
		return adv, tok, err
	}
}