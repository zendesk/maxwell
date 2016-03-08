package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.filter.MaxwellColumnFilter;

public class MaxwellFilteringProducer extends AbstractProducer {

    private final MaxwellColumnFilter filter;
    private final AbstractProducer delegate;

    public MaxwellFilteringProducer(MaxwellContext context, AbstractProducer abstractProducer) {
        super(context);
        filter = new MaxwellColumnFilter(context.getConfig().configLocation);
        this.delegate = abstractProducer;
    }

    @Override
    public void push(RowMap r) throws Exception {
        filter.applyFilter(r);
        delegate.push(r);
    }

}
