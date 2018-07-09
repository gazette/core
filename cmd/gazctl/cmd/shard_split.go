package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"

	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/journal"
	rocks "github.com/tecbot/gorocksdb"
	"io"
	"strconv"
)

var shardSplitCmd = &cobra.Command{
	Use:   "split [rocsksdb-directory] [local-output-path] [current-shard-number] [current-total-shards] [consumer-journal]",
	Short: "Splits a shard into multiple smaller parts containing a subset of the keyspace.",
	Long: `Split takes the set of files holding the data in a rocksdb instance and iterates through all keys,
splitting them into some number of new partitions. The data for each new partition will be written to files
in the local output path as pairs of hex-encoded keys and values, in a format that can be directly read by shard_compose.

By default, this function will split a shard in half arbitrarily, by taking a hashmod. You can split shards in a custom way by
adding a config called "split.plugin" pointing to a .so plugin containing an implementation of the Split type 
as defined in plugin_types.go.'
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 5 {
			cmd.Usage()
			log.Fatal("invalid arguments")
		}
		var rocksdbPath, outputPath = args[0], args[1]
		currentShard, err := strconv.Atoi(args[2])
		if err != nil {
			log.Fatal("invalid argument: current-shard-number is not an integer")
		}
		totalShards, err := strconv.Atoi(args[3])
		if err != nil {
			log.Fatal("invalid argument: current-total-shards is not an integer")
		}
		var consumerJournal = args[4]

		var opts = rocks.NewDefaultOptions()
		if plugin := consumerPlugin(); plugin != nil {
			if initer, _ := plugin.(consumer.OptionsIniter); initer != nil {
				initer.InitOptions(opts)
			}
		}

		var db *rocks.DB
		var iter iterFunc
		if db, err = rocks.OpenDbForReadOnly(opts, rocksdbPath, true); err == nil {
			iter = newDBIterFunc(db)
		}

		defer func() {
			db.Close()
		}()

		plugin := splitPlugin()
		var splitFunction Split
		if plugin != nil {
			splitFunction = plugin
		} else {
			splitFunction = Halve
		}

		partitioner := Partitioner{
			partitions:         make(map[int]*bufio.Writer),
			outputDir:          outputPath,
			split:              splitFunction,
			currentShard:       currentShard,
			currentTotalShards: totalShards,
			consumerJournal:    journal.Name(consumerJournal),
		}

		var key, value []byte
		for key, value = nil, nil; err == nil; key, value, err = iter(key, value) {
			partitioner.Assign(key, value)
		}
		if err != io.EOF {
			log.WithField("err", err).Fatal("failed to split full DB")
		}
		err = partitioner.Flush()
		if err != nil {
			log.WithField("err", err).Fatal("failed to flush writers")
		}
	},
}

type Partitioner struct {
	partitions         map[int]*bufio.Writer
	outputDir          string
	split              Split
	currentShard       int
	currentTotalShards int
	consumerJournal    journal.Name
}

func (p Partitioner) Assign(key []byte, value []byte) {
	if len(key) == 0 || len(value) == 0 {
		return // filter our nil or garbage keys
	}
	var part int
	if offsetKey := consumer.AppendOffsetKeyEncoding(nil, p.consumerJournal); bytes.Equal(key, offsetKey) {
		// we want to keep the offset in the current shard. This is so that in the case where we are starting up the consumer
		// again from the split shard databases, the proper shards will remember at what point new data starts in their root
		// journals, and we won't have to read them starting from the beginning of time.
		part = p.currentShard
	} else {
		part = p.split(p.currentShard, key)
	}

	if _, hasKey := p.partitions[part]; !hasKey {
		outputPath := fmt.Sprintf("%s/shard-compose-%d-%d", p.outputDir, p.currentShard, part)
		writer, err := os.Create(outputPath)
		if err != nil {
			panic("Failed to initialize writer at path " + outputPath)
		} else {
			p.partitions[part] = bufio.NewWriter(writer)
		}
	}
	p.partitions[part].WriteString(asHexLine(key, value))
}

func asHexLine(key []byte, value []byte) string {
	return "0x" + hex.EncodeToString(key) + " 0x" + hex.EncodeToString(value) + "\n"
}

func (p Partitioner) Flush() error {
	for writer := range p.partitions {
		if err := p.partitions[writer].Flush(); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	shardCmd.AddCommand(shardSplitCmd)
}
