package main

import (
	"github.com/jessevdk/go-flags"

	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	mbp "go.gazette.dev/core/mainboilerplate"
)

const iniFilename = "gazctl.ini"

func main() {
	parser := flags.NewParser(nil, flags.Default)

	mbp.AddPrintConfigCmd(parser, iniFilename)
	parser.LongDescription = `gazctl is a tool for interacting with Gazette brokers and consumer applications.

	See --help pages of each sub-command for documentation and usage examples.
	Optionally configure gazctl with a '` + iniFilename + `' file in the current working directory,
	or with '~/.config/gazette/` + iniFilename + `'. Use the 'print-config' sub-command to inspect
	the tool's current configuration.
	`

	// Subcommands that exist solely to contain and organize further nested
	// subcommands; i.e., they do nothing when executed. They must be
	// initialized here so they exist prior to any init() functions being
	// called to add nested subcommands.
	cmdJournals := mustAddCmd(parser.Command, "journals", "Interact with broker journals", "", gazctlcmd.JournalsCfg)
	for _, addSubCommand := range gazctlcmd.JournalRegisterCommands {
		mbp.Must(addSubCommand(cmdJournals), "could not add journals subcommand")
	}

	cmdShards := mustAddCmd(parser.Command, "shards", "Interact with consumer shards", "", gazctlcmd.ShardsCfg)
	for _, addSubCommand := range gazctlcmd.ShardRegisterCommands {
		mbp.Must(addSubCommand(cmdShards), "could not add shards subcommand")
	}

	mbp.Must(gazctlcmd.AddCmdAttachUUIDs(parser.Command), "could not add attach-uuids subcommand")

	mbp.MustParseConfig(parser, iniFilename)
}

func mustAddCmd(cmd *flags.Command, name, short, long string, cfg interface{}) *flags.Command {
	cmd, err := cmd.AddCommand(name, short, long, cfg)
	mbp.Must(err, "failed to add command")
	return cmd
}
