package com.zendesk.maxwell;

import org.junit.runner.Description;
import org.junit.runner.FilterFactory;
import org.junit.runner.FilterFactoryParams;
import org.junit.runner.manipulation.Filter;

/**
 * Created by ben on 12/26/16.
 */
public class JUnitNameFilterFactory implements FilterFactory {
	private class JUnitNameFilter extends Filter {
		private final String filterString;

		private JUnitNameFilter(String filterString) {
			this.filterString = filterString;
		}
		@Override
		public boolean shouldRun(Description description) {
			String methodName = description.getMethodName();
			if ( methodName == null )
				return true;
			return filterString.equalsIgnoreCase(methodName);
		}

		@Override
		public String describe() {
			return "name = " + filterString;
		}
	}
	@Override
	public Filter createFilter(FilterFactoryParams params) throws FilterNotCreatedException {
		return new JUnitNameFilter(params.getArgs().toString());
	}
}
