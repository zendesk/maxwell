package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.util.AbstractConfig;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by springloops on 2016. 8. 31..
 */
public class HAConfig extends AbstractConfig {

	private static final String DEFAULT_HA_CONFIG_FILE = "ha.config.properties";

	private MaxwellConfig maxwellConfig;
	private String zkAddress;
	private String clusterName;
	private String instanceName;
	private String nodeHostName;
	private String clusterPort;
	private Boolean startController;

	public HAConfig(String[] args) throws UnknownHostException {
		this.parse(args);
		this.maxwellConfig = new MaxwellConfig(args);
		//this.validate();
	}

	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.allowsUnrecognizedOptions();

		parser.accepts( "zkAddress", "When ACTIVE_STANDBY mode, determined zookeeper address, formatted as —-zkAddress=zkIP:port[,skip:port]…" ).withRequiredArg();
		parser.accepts( "clusterName", "When ACTIVE_STANDBY mode, determined maxwell cluster name, formatted as —-clusterName=<ClusterName>" ).withRequiredArg();
		parser.accepts( "instanceName", "When ACTIVE_STANDBY mode, determined each maxwell node name, formatted as —-instanceName=<unique maxwell node name>" ).withRequiredArg();
		parser.accepts( "nodeHostName", "When ACTIVE_STANDBY mode, determined node's network hostname, formatted as —-nodeHostName=<hostName>. default value is `InetAddress.getLocalHost().getHostName()`" ).withOptionalArg();
		parser.accepts( "clusterPort", "When ACTIVE_STANDBY mode, determined communication port for between nodes, formatted as —-clusterPort=<using port>. default value is 12000" ).withOptionalArg();
		parser.accepts( "startController", "When ACTIVE_STANDBY mode, determined whether Helix Controller, formatted as --startController=true|false. default value is true" ).withOptionalArg();

		parser.accepts( "help", "display help").forHelp();

		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				return output.replaceAll("--__separator_.*", "");
			}
		};

		parser.formatHelpWith(helpFormatter);
		return parser;
	}

	private void parse(String[] args) throws UnknownHostException {
		OptionSet options = buildOptionParser().parse(args);

		Properties properties;

		if (options.has("haconfig")) {
			properties = parseFile((String) options.valueOf("haconfig"), true);
		} else {
			properties = parseFile(DEFAULT_HA_CONFIG_FILE, false);
		}

		if (options.has("help"))
			usage("Help for Active-Standby Maxwell:");

		setup(options, properties);
	}

	private void setup(OptionSet options, Properties properties) throws UnknownHostException {
		this.zkAddress = fetchOption("zkAddress",options, properties, "localhost:2181");
		this.clusterName = fetchOption("clusterName", options, properties, "maxwell");
		this.instanceName = fetchOption("instanceName", options, properties, "maxwell");
		this.nodeHostName = fetchOption("nodeHostName", options, properties, InetAddress.getLocalHost().getHostName());
		this.clusterPort = fetchOption("clusterPort", options, properties, "12000");
		this.startController = fetchBooleanOption("startController", options, properties, true);
	}

	@Override
	protected void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			maxwellConfig.buildOptionParser().printHelpOn(System.err);
			buildOptionParser().printHelpOn(System.err);
			System.exit(1);
		} catch (IOException e) { }
	}

	private Properties parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			p = new Properties();

		return p;
	}


	private void validate(){
		//TODO..
	}

	public String getZkAddress() {
		return zkAddress;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public String getNodeHostName() {
		return nodeHostName;
	}

	public String getClusterPort() {
		return clusterPort;
	}

	public Boolean getStartController() {
		return startController;
	}

	public MaxwellConfig getMaxwellConfig() {
		return maxwellConfig;
	}
}
