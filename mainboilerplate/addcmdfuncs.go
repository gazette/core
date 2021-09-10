package mainboilerplate

import "github.com/jessevdk/go-flags"

// Functions used to register sub-commands with a parent
type AddCmdFunc func(*flags.Command) error

// This is a simple manager to allow sub-commands to auto-register with a parent command at runtime
type AddCmdFuncManager map[string][]AddCmdFunc

// NewAddCmdFuncManager creates a new manager
func NewAddCmdFuncManager() AddCmdFuncManager {
	return make(AddCmdFuncManager)
}

// RegisterAddCmdFunc registers an AddCmdFunc. parentName should separate mulitple sub-commands with periods.
// Example command.subcommand1.subcommand2
func (acfh AddCmdFuncManager) RegisterAddCmdFunc(parentName string, f AddCmdFunc) {
	acfh[parentName] = append(acfh[parentName], f)
}

// RegisterCmds recursively walks through the tree of registered sub-commands under parent-name and adds them to the root comamnd.
func (acfh AddCmdFuncManager) RegisterCmds(rootName string, root *flags.Command) error {
	baseName := rootName
	if baseName != "" {
		baseName += "."
	}
	for _, cmd := range root.Commands() {
		cmdName := baseName + cmd.Name
		// Register every sub-command
		for _, addCmdFunc := range acfh[cmdName] {
			if err := addCmdFunc(cmd); err != nil {
				return err
			}
		}
		// Recursively register any sub-commands under this command
		if err := acfh.RegisterCmds(cmdName, cmd); err != nil {
			return err
		}
	}
	return nil
}
