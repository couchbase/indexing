package protobuf

func (pl *Payload) Value() interface{} {
	if pl.Vbmap != nil {
		return pl.Vbmap
	} else if pl.Vbkeys != nil {
		return pl.Vbkeys
	}
	return nil
}
