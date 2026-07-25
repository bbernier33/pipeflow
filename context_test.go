package pipeflow

import "testing"

func TestContextSetAndGet(t *testing.T) {
	ctx := NewContext()

	ctx.Set("foo", "bar")
	value, exists := ctx.Get("foo")

	if !exists {
		t.Fatal("expected value to exist")
	}
	if value != "bar" {
		t.Errorf("expected bar, got %v", value)
	}
}

func TestContextGetMissingKey(t *testing.T) {
	ctx := NewContext()

	_, exists := ctx.Get("missing")
	if exists {
		t.Fatal("expected value to not exist")
	}

}

func TestPipelineSharesContextAcressSteps(t *testing.T) {
	ctx := NewContext()

	stepOne := NewStep("Set Value", func(ctx *Context, input any) (any, error) {
		ctx.Set("count", 10)
		return input, nil
	})

	stepTwo := NewStep("Get Value", func(ctx *Context, input any) (any, error) {
		value, exists := ctx.Get("count")

		if !exists {
			t.Fatal("expecgted count to exist")
		}

		return value, nil
	})

	stageOne := NewStage("Stage One", stepOne)
	stageTwo := NewStage("Stage Two", stepTwo)

	pipeline := NewPipeline("Pipeline", stageOne, stageTwo)

	output, err := pipeline.Run(ctx, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if output.(int) != 10 {
		t.Errorf("expected output 10, got %v", output)
	}

}
