package blocks

type Connection struct {
	Block
}

func (b *Connection) setup() error {
	return nil
}

func TransformMessage(msg MsgData) (MsgData, error) {
	return msg, nil
}
