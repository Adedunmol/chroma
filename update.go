package chroma

type Update struct{}

func NewUpdate() Update {

	return Update{}
}

func (u *Update) Parse(data []byte) (*Update, error) {

	return u, nil
}
