package pipeflow

type Context struct {
	values map[string]any
}

func NewContext() *Context {
	return &Context{
		values: make(map[string]any),
	}
}

func (c *Context) Set(key string, value any) {
	c.values[key] = value
}

func (c *Context) Get(key string) (any, bool) {
	value, exists := c.values[key]
	return value, exists
}
