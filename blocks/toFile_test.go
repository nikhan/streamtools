package blocks

import (
	"log"
	"testing"
	"time"
)

func TestBlock(t *testing.T) {
	inchan := make(chan Msg)
	outchan := make(chan Msg)
	errchan := make(chan error)
	quitchan := make(chan bool)
	exitchan := make(chan bool)
	base := Block{
		Name:     "testingBlock",
		InChan:   inchan,
		OutChan:  outchan,
		ErrChan:  errchan,
		QuitChan: quitchan,
		ExitChan: exitchan,
	}
	b := &ToFile{
		Block: base,
	}
	setrulemsg := Msg{
		msg:   map[string]interface{}{"Filename": "test.out"},
		route: "setrule",
	}
	go BlockRoutine(b)
	inchan <- setrulemsg
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		quitchan <- true
	})
	for {
		select {
		case err := <-errchan:
			log.Println(err.Error())
		case <-exitchan:
			return
		}
	}

}
