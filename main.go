package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"code.sajari.com/docconv"
	"github.com/lunixbochs/struc"
	"gopkg.in/jdkato/prose.v2"
)

// Build large index on Disk
// Struct for index entry - X
// Build terms dictionary - X
// Build docs dictionary - X
// Block of indexes - sort by term ID
// Swap block on disk
// Start another block
// Write all blocks on disk
// Merge blocks in one index

var punctuation = []string{".", "!", "?", ":", ";", ",", "(", ")", "—", "·"}

// Terms - mapping from term to term ID
type Terms map[string]int32

// Docs - mapping from doc to doc ID
type Docs map[int32]string

// Record - processing record
type Record struct {
	TermID int32
	DocID  int32
}

// InvIndexEntry - entry to the inverted index
type InvIndexEntry struct {
	TermID       int32
	Size         int32 `struc:"sizeof=InvertedList"`
	InvertedList []int32
}

// InvIndexShard - a small block of term-documents inverted indices
type InvIndexShard struct {
	Size  int32 `struc:"sizeof=Terms"`
	Terms []InvIndexEntry
}

// InvIndex - terms documents inverted index
type InvIndex struct {
	Size   int32 `struc:"sizeof=Shards"`
	Shards []InvIndexShard
}

// IsIn - checks if the value is in a slice
func IsIn(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// FileExists - returns true if file exists and is not a folder
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func writeBlock(blockIndex int, blockRecords []Record, blockShardsCount *[]int) {
	// Sort block records
	sort.Slice(blockRecords, func(i, j int) bool {
		return blockRecords[i].TermID < blockRecords[j].TermID
	})

	// Create inverted index for each term and write on disk sharded
	shardSize := 1000
	shards := make([]InvIndexShard, 1, 100)
	currentTermID := int32(-1)
	var currentShard *InvIndexShard = &shards[0]
	var currentEntry *InvIndexEntry

	for _, entry := range blockRecords {
		if entry.TermID != currentTermID { // New term encoutered - start a new inverted index entry
			if len(currentShard.Terms) == shardSize { // Current shard is done - start a new shard
				shards = append(shards, InvIndexShard{})
				currentShard = &shards[len(shards)-1]
			}

			currentTermID = entry.TermID

			currentShard.Size++
			currentShard.Terms = append(currentShard.Terms,
				InvIndexEntry{ // Start a new index entry
					TermID:       currentTermID,
					Size:         0,
					InvertedList: make([]int32, 0, 100),
				})

			currentEntry = &currentShard.Terms[currentShard.Size-1]
		}

		currentEntry.Size++
		currentEntry.InvertedList = append(currentEntry.InvertedList, entry.DocID)
	}

	blockFolder := "blocks/block-" + strconv.Itoa(blockIndex)
	os.MkdirAll(blockFolder, os.ModePerm)

	var buf bytes.Buffer
	for i, shard := range shards {
		writer, err := os.Create(blockFolder + "/shard-" + strconv.Itoa(i) + ".bin")
		if err != nil {
			panic(err)
		}

		defer writer.Close()

		err = struc.Pack(&buf, &shard)
		if err != nil {
			fmt.Println("Packing failed:", err)
		}

		buf.WriteTo(writer)
	}

	*blockShardsCount = append(*blockShardsCount, len(shards))
}

// CreateInvIndex - Create inverse index of words for text files in a directory
func CreateInvIndex(filesDir string) (terms Terms, docs Docs) {
	i := 0
	blockIndex := 0
	blockSize := 10000
	terms = make(Terms)
	docs = make(Docs)
	blockRecords := make([]Record, blockSize)

	// ============== Build inverted index blocks ==============
	var blockShardsCount []int // number of shards in each block

	files, err := filepath.Glob(filesDir + "/*")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexing %v documents.", len(files))

	for _, file := range files {
		docID := int32(len(docs))
		docs[docID] = file // assign document and ID

		res, err := docconv.ConvertPath(file) // get document handler
		if err != nil {
			log.Fatal(err)
		}

		doc, err := prose.NewDocument(res.Body, prose.WithExtraction(false), prose.WithTagging(false)) // read document
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("%v: Found %v tokens.", file, len(doc.Tokens()))

		for _, token := range doc.Tokens() {
			if !IsIn(punctuation, token.Text) {
				if _, ok := terms[token.Text]; !ok {
					terms[token.Text] = int32(len(terms)) // Assign term and ID
				}

				termID := terms[token.Text]
				blockRecords[i] = Record{
					TermID: termID,
					DocID:  docID,
				}

				if i < blockSize-1 {
					i++
				} else { // Current block is done - write on disk and start a new block
					writeBlock(blockIndex, blockRecords[0:i], &blockShardsCount)
					blockIndex++
					i = 0
				}
			}
		}

		if i != 0 { // write the last uncompleted block
			writeBlock(blockIndex, blockRecords[0:i], &blockShardsCount)
			blockIndex++
		}
	}

	// fmt.Printf("%v", blockRecords)

	// ============== Merge blocks into one index file ==============

	i = 0
	indexShardSize := 10000
	blocksCount := blockIndex
	currentShardIndex := make([]int, blocksCount)  // index of the currently processed shard in each block
	currentTermIndex := make([]int32, blocksCount) // index of the first unprocessed yet term in each shard
	shards := make([]InvIndexShard, blocksCount)   // the list of currrently loaded shard for each block
	blockFinished := make([]bool, blocksCount)     // the list of falgs for finished blocks

	os.MkdirAll("index", os.ModePerm)

	var buf bytes.Buffer
	var indexShard InvIndexShard
	for {
		minTermID := int32(math.MaxInt32)
		for i := range shards {
			if currentTermIndex[i] == shards[i].Size { // if the end of the shard is reached
				if blockShardsCount[i] > 0 { // and if block still has more unprocessed shards
					blockShardsCount[i]--

					// refil shard
					blockFolder := "blocks/block-" + strconv.Itoa(i)
					reader, err := os.Open(blockFolder + "/shard-" + strconv.Itoa(currentShardIndex[i]) + ".bin")
					if err != nil {
						fmt.Println("Openning failed:", err)
					}

					defer reader.Close()

					buf.ReadFrom(reader)
					err = struc.Unpack(&buf, &shards[i])
					if err != nil {
						fmt.Println("Unpacking failed:", err)
					}

					currentShardIndex[i]++
					currentTermIndex[i] = 0
				} else {
					blockFinished[i] = true
					continue
				}
			}

			if shards[i].Terms[currentTermIndex[i]].TermID < minTermID {
				minTermID = shards[i].Terms[currentTermIndex[i]].TermID
			}
		}

		if minTermID == int32(math.MaxInt32) { // if all terms in all blocks has been processed
			indexWritter, err := os.Create("index/shard-" + strconv.Itoa(i) + ".bin")
			if err != nil {
				panic(err)
			}

			defer indexWritter.Close()

			err = struc.Pack(&buf, &indexShard)
			if err != nil {
				fmt.Println("Packing failed:", err)
			}
			buf.WriteTo(indexWritter)

			indexShard = InvIndexShard{}

			break
		}

		indexShard.Terms = append(indexShard.Terms, InvIndexEntry{
			TermID:       minTermID,
			Size:         0,
			InvertedList: make([]int32, 0, 100),
		})

		term := &indexShard.Terms[len(indexShard.Terms)-1]

		docIDSet := make(map[int32]struct{})
		for i, shard := range shards {
			if !blockFinished[i] {
				shardCurrentTerm := &shard.Terms[currentTermIndex[i]]
				if shardCurrentTerm.TermID == minTermID {
					for _, docID := range shardCurrentTerm.InvertedList {
						docIDSet[docID] = struct{}{}
					}

					currentTermIndex[i]++
				}
			}
		}

		for docID := range docIDSet {
			term.InvertedList = append(term.InvertedList, docID)
		}
		term.Size = int32(len(term.InvertedList))

		if len(indexShard.Terms) == indexShardSize {
			indexWritter, err := os.Create("index/shard-" + strconv.Itoa(i) + ".bin")
			if err != nil {
				panic(err)
			}

			defer indexWritter.Close()

			err = struc.Pack(&buf, &indexShard)
			if err != nil {
				fmt.Println("Packing failed:", err)
			}
			buf.WriteTo(indexWritter)

			indexShard = InvIndexShard{}
			i++
		}
	}

	return
}

// ReadIndexShard - reads shard
func ReadIndexShard(i int) (shard InvIndexShard) {
	reader, err := os.Open("index/shard-" + strconv.Itoa(i) + ".bin")
	if err != nil {
		fmt.Println("Openning failed:", err)
	}

	defer reader.Close()

	var buf bytes.Buffer
	buf.ReadFrom(reader)
	err = struc.Unpack(&buf, &shard)
	if err != nil {
		fmt.Println("Unpacking failed:", err)
	}

	return
}

// FindTerm - searches for a term in the inverted index
func FindTerm(term string, terms Terms, docs Docs) {
	docNames := make([]string, 0)
	termID, ok := terms[term]
	if ok {
		i := 0
		for len(docNames) == 0 {
			shard := ReadIndexShard(i)
			for _, term := range shard.Terms {
				if term.TermID == termID {
					for _, docID := range term.InvertedList {
						docNames = append(docNames, docs[docID])
					}
				}
			}
			i++
		}
	}

	fmt.Printf("%v\n", docNames)
}

func main() {
	terms, docs := CreateInvIndex("./texts")
	FindTerm("FORTRAN", terms, docs)
	FindTerm("Герой", terms, docs)
	FindTerm("physics", terms, docs)
}
