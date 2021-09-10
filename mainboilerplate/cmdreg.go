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

// RegisterCmd takes a parentName and then an github.com/jessevdk/go-flags.AddCommand specification and registers the command
func (cr CmdRegistry) RegisterCmd(parentName string, command string, shortDescription string, longDescription string, data interface{}) {
	cr.RegisterAddCmdFunc(parentName, func(cmd *flags.Command) error {
		_, err := cmd.AddCommand(command, shortDescription, longDescription, data)
		return err
	})
}

// RegisterAddCmdFunc registers an AddCmdFunc. parentName should separate mulitple sub-commands with periods.
// Example command.subcommand1.subcommand2
func (cr CmdRegistry) RegisterAddCmdFunc(parentName string, f AddCmdFunc) {
	cr[parentName] = append(cr[parentName], f)
}

// AddCmds recursively walks through the tree of registered sub-commands under parent-name and adds them to the root comamnd.
func (cr CmdRegistry) AddCmds(rootName string, rootCmd *flags.Command) error {

	// Register any command
	for _, addCmdFunc := range cr[rootName] {
		if err := addCmdFunc(rootCmd); err != nil {
			return err
		}
	}

	// See if there are any sub-commands that now need registered under this one
	for _, cmd := range rootCmd.Commands() {
		// Register every sub-command. Sub-Commands are separated with a .
		cmdName := cmd.Name
		if rootName != "" {
			cmdName = rootName + "." + cmdName
		}
		// Recursively register any sub-commands under this command
		if err := cr.AddCmds(cmdName, cmd); err != nil {
			return err
		}
	}

	return nil

}
