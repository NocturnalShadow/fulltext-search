package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"code.sajari.com/docconv"
	"github.com/lunixbochs/struc"
	"gopkg.in/jdkato/prose.v2"
)

var punctuation = []string{".", "!", "?", ":", ";", ",", "(", ")", "—", "·"}

// Build large index on Disk
// Struct for index entry - X
// Build terms dictionary - X
// Build docs dictionary - X
// Block of indexes - sort by term ID
// Swap block on disk
// Start another block
// Write all blocks on disk
// Merge blocks in one index

// Terms - mapping from term to term ID
type Terms map[string]int32

// Docs - mapping from doc to doc ID
type Docs map[string]int32

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

// CreateInvIndex - Create inverse index of words for text files in a directory
func CreateInvIndex(filesDir string) {
	i := 0
	blockID := 0
	blockSize := 100000
	shardSize := 1000
	terms := make(Terms)
	docs := make(Docs)
	recordsBlock := make([]Record, blockSize)

	// Build inverted index blocks
	files, err := filepath.Glob(filesDir + "/*")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexing %v documents.", len(files))

	for _, file := range files {
		docID := int32(len(docs))
		docs[file] = docID // assign document and ID

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
				recordsBlock[i] = Record{
					TermID: termID,
					DocID:  docID,
				}

				if i < blockSize-1 {
					i++
				} else { // Current block is done - write on disk and start a new block
					// Sort block records
					sort.Slice(recordsBlock[0:i], func(i, j int) bool {
						return recordsBlock[i].TermID < recordsBlock[j].TermID
					})

					// Create inverted index for each term and write on disk sharded
					shards := make([]InvIndexShard, 1, 100)
					currentTermID := int32(-1)
					var currentShard *InvIndexShard = &shards[0]
					var currentEntry *InvIndexEntry

					for _, entry := range recordsBlock {
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

					fileWritter, err := os.Create("index/block-" + strconv.Itoa(blockID) + ".bin")
					if err != nil {
						panic(err)
					}

					defer fileWritter.Close()

					var buf bytes.Buffer

					shardsCount := int32(len(shards))
					err = struc.Pack(&buf, &shardsCount)
					if err != nil {
						fmt.Println("Packing failed:", err)
					}

					buf.WriteTo(fileWritter)

					for _, shard := range shards {
						err = struc.Pack(&buf, &shard)
						if err != nil {
							fmt.Println("Packing failed:", err)
						}

						buf.WriteTo(fileWritter)
					}

					blockID++
					i = 0
				}
			}
		}
	}

	// fmt.Printf("%v", recordsBlock)

	// Merge blocks into one index file
	files, err = filepath.Glob(dir + "/*")
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	var blockReaders []*os.File
	var blockSizes []int32
	for _, file := range files {
		reader, err := os.Open(file)
		if err != nil {
			fmt.Println("Unpacking failed:", err)
		}

		var size int32
		buf.Reset()
		buf.ReadFrom(reader)
		err = struc.Unpack(&buf, &size)
		if err != nil {
			fmt.Println("Unpacking failed:", err)
		}

		blockReaders = append(blockReaders, reader)
		blockSizes = append(blockSizes, size)
	}

	findexWritter, err := os.Create("index/inv-index.bin")
	if err != nil {
		panic(err)
	}

	defer findexWritter.Close()

	blocksCount := len(blockReaders)                  // number of blocks
	blockShards := make([]InvIndexShard, blocksCount) // currently processed shards for each block
	blockShardIndex := make([]int32, blocksCount)     // currently processed entry in a shard

	var indexShard InvIndexShard
	for {
		minTermID := int32(-1)
		for i, shard := range blockShards {
			if blockShardIndex[i] == shard.Size {
				if blockSizes[i] > 0 {
					blockSizes[i]--
					buf.Reset()
					buf.ReadFrom(blockReaders[i])
					err := struc.Unpack(&buf, &blockShards[i])
					if err != nil {
						fmt.Println("Unpacking failed:", err)
					}
				} else {
					continue
				}
			}

			if shard.Terms[blockShardIndex[i]].TermID < minTermID {
				minTermID = shard.Terms[blockShardIndex[i]].TermID
			}
		}

		if minTermID == int32(-1) {
			break
		}

		indexShard.Terms = append(indexShard.Terms, InvIndexEntry{
			TermID:       minTermID,
			Size:         0,
			InvertedList: make([]int32, 0, 100),
		})

		term := &indexShard.Terms[len(indexShard.Terms)-1]

		for i, shard := range blockShards {
			if shard.Terms[i].TermID == minTermID {
				blockShardIndex[i]++
				term.InvertedList = append(term.InvertedList, shard.Terms[i].InvertedList...)
			}
		}

		term.Size = int32(len(term.InvertedList))

		if len(indexShard.Terms) == shardSize {
			buf.Reset()
			err = struc.Pack(&buf, &indexShard)
			if err != nil {
				fmt.Println("Packing failed:", err)
			}
			buf.WriteTo(findexWritter)
		}
	}
}

func main() {
	CreateInvIndex("./texts")
}
