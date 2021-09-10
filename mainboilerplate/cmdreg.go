package mainboilerplate

import "github.com/jessevdk/go-flags"

// Functions used to register sub-commands with a parent
type AddCmdFunc func(*flags.Command) error

// CmdRegistry is a simple tool to allow sub-commands to auto-register with a parent command at runtime
type CmdRegistry map[string][]AddCmdFunc

// NewCmdRegistry creates a new registry
func NewCmdRegistry() CmdRegistry {
	return make(CmdRegistry)
}

// RegisterAddCmdFunc registers an AddCmdFunc. parentName should separate mulitple sub-commands with periods.
// Example command.subcommand1.subcommand2
func (cr CmdRegistry) RegisterAddCmdFunc(parentName string, f AddCmdFunc) {
	cr[parentName] = append(cr[parentName], f)
}

// AddCmds recursively walks through the tree of registered sub-commands under parent-name and adds them to the root comamnd.
func (cr CmdRegistry) AddCmds(rootName string, rootCmd *flags.Command) error {
	baseName := rootName
	if baseName != "" {
		baseName += "."
	}
	for _, cmd := range rootCmd.Commands() {
		cmdName := baseName + cmd.Name
		// Register every sub-command
		for _, addCmdFunc := range cr[cmdName] {
			if err := addCmdFunc(cmd); err != nil {
				return err
			}
		}
		// Recursively register any sub-commands under this command
		if err := cr.AddCmds(cmdName, cmd); err != nil {
			return err
		}
	}
	return nil
}
