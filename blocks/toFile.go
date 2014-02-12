package blocks

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
	file *os.File // this is the file to write to
}

// setup is called before beginning the BlockRoutine's main loop. The only thing
// we must do is specify the Rule if there is one.
func (b *ToFile) setup() error {
	b.Rule = map[string]interface{}{
		"Filename": "",
	}
	b.Kind = "toFile"
	return nil
}

// SetRule can be overriden if anything specific needs to happen using the new
// rule.
func (b *ToFile) setRule(msg MsgData) error {
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
	b.file = fo
	return nil
}

// specify each behaviour that the block should perform
func (b *ToFile) WriteExternalMessage(msg MsgData) error {
	if b.file == nil {
		return nil
	}
	msgStr, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(b.file)
	fmt.Fprintln(w, string(msgStr))
	// consider making the per-message flush optional, move the writer into the
	// object and Flush in tidyup.
	w.Flush()
	return nil
}

// this is called when the block is asked to quit
func (b *ToFile) tidyUp() error {
	return b.file.Close()
}
