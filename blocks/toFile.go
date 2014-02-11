package newblocks

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// this embeds Block and specifies any other bits and bobs requires by this
// specific block
type ToFile struct {
	Block
	w *bufio.Writer
}

// setup is called before beginning the BlockRoutine's main loop. The only thing
// we must do is specify the Rule if there is one.
func (b *ToFile) Setup() error {
	b.Rule = map[string]interface{}{
		"Filename": "",
	}
	return nil
}

// SetRule can be overriden if anything specific needs to happen using the new
// rule.
func (b ToFile) SetRule(msg MsgData) error {
	filenameInterface, ok := msg["Filename"]
	if !ok {
		return errors.New("Rule message did not contain Filename")
	}
	filename, ok := filenameInterface.(string)
	if !ok {
		return errors.New("could not assert filename to a string")
	}
	fo, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fo.Close()
	b.w = bufio.NewWriter(fo)
	return nil
}

// specify each behaviour that the block should perform
func (b ToFile) WriteExternalMessage(msg MsgData) error {
	if b.w == nil {
		return nil
	}
	msgStr, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	fmt.Fprintln(b.w, string(msgStr))
	b.w.Flush()
	return nil
}
