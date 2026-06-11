package streamaggr

type sumSamplesAggrValue struct {
	sum float64
}

func (av *sumSamplesAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.sum += sample.value
}

func (av *sumSamplesAggrValue) flush(c aggrConfig, ctx *flushCtx, key string, _ bool) {
	ac := c.(*sumSamplesAggrConfig)
	if ac.resetTotalOnFlush {
		ctx.appendSeries(key, "sum_samples", av.sum)
		av.sum = 0
		return
	}
	ctx.appendSeries(key, "sum_samples_total", av.sum)
}

func (*sumSamplesAggrValue) state() any {
	return nil
}

func newSumSamplesAggrConfig(resetTotalOnFlush bool) aggrConfig {
	return &sumSamplesAggrConfig{
		resetTotalOnFlush: resetTotalOnFlush,
	}
}

type sumSamplesAggrConfig struct {
	resetTotalOnFlush bool
}

func (*sumSamplesAggrConfig) getValue(_ any) aggrValue {
	return &sumSamplesAggrValue{}
}
