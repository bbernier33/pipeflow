package pipeflow

type Context struct {
	values map[string]any
	logger Logger
}

func NewContext() *Context {
	return &Context{
		values: make(map[string]any),
		logger: DefaultLogger{},
	}
}

func NewContextWithLogger(logger Logger) *Context {
	return &Context{
		values: make(map[string]any),
		logger: logger,
	}
}

func (c *Context) Set(key string, value any) {
	c.values[key] = value
}

func (c *Context) Logger() Logger {
	return c.logger
}

func (c *Context) Get(key string) (any, bool) {
	value, exists := c.values[key]
	return value, exists
}
