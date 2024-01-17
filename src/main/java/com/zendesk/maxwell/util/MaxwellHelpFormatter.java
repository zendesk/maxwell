package com.zendesk.maxwell.util;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionSpecBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaxwellHelpFormatter extends BuiltinHelpFormatter {
	private String section = null;
	private Map<String, ArrayList<String>> sections;
	private List<String> sectionNames;

	public MaxwellHelpFormatter(
		int desiredOverallWidth,
		int desiredColumnSeparatorWidth,
		Map<String, ArrayList<String>> sections,
		List<String> sectionNames
	) {
		super(desiredOverallWidth, desiredColumnSeparatorWidth);
		this.sections = sections;
		this.sectionNames = sectionNames;
	}


	@Override
	public String format(Map<String, ? extends OptionDescriptor> options) {
		if ( section != null && section.equalsIgnoreCase("all") )
			this.addRows(options.values());
		else {
			ArrayList<String> list = sections.get(section);
			if ( list == null ) {
				return "Unknown help section: " + section + "\n";
			}

			ArrayList l = new ArrayList();

			// add the "non-options" arg-spec.
			l.add(options.values().toArray()[0]);

			for ( String name : list ) {
				l.add(options.get(name));
			}

			this.addRows(l);
		}

		String output = this.formattedHelpOutput();
		output = output.replaceAll("--__separator_.*", "");

		Matcher matcher = Pattern.compile("--__section_(\\w+)\\s*").matcher(output);
		StringBuffer result = new StringBuffer();

		while (matcher.find()) {
			String sectionName = matcher.group(1);
			matcher.appendReplacement(result, "\n" + matcher.group(1) + " options:\n");
			result.append(StringUtils.repeat('-', matcher.group(1).length() + " options:".length()) + "\n");
		}
		matcher.appendTail(result);

		output = result.toString();
		output = output + "\n--help [ all, "
			+ StringUtils.join(sectionNames, ", ") +  " ]\n";

		Pattern deprecated = Pattern.compile("^.*\\[deprecated\\].*\\n", Pattern.MULTILINE);
		return deprecated.matcher(output).replaceAll("");
	}

	public void setSection(String section) {
		this.section = section;
	}
}
