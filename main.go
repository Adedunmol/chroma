package chroma

type Handler interface {
	Parse(map[string]interface{}) error
	String() string
}
