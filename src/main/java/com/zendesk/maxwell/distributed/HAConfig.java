package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.util.AbstractConfig;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.Map;
import java.util.Properties;

/**
 * Created by springloops on 2016. 8. 31..
 */
public class HAConfig extends AbstractConfig {

	private static final String DEFAULT_HA_CONFIG_FILE = "ha.config.properties";

	private String zkAddress;
	private String clusterName;
	private String instanceName;
	private String hostName;
	private String clusterPort;
	private Boolean startController;

	public HAConfig(String[] args) {
		this.parse(args);
		//this.validate();
	}

	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();

		parser.accepts( "zkAddress", "When ACTIVE_STANDBY mode, determined zookeeper address, formatted as —zkAddress=zkIP:port[,skip:port]…" ).withRequiredArg();
		parser.accepts( "clusterName", "When ACTIVE_STANDBY mode, determined maxwell cluster name, formatted as —clusterName=<ClusterName>" ).withRequiredArg();
		parser.accepts( "instanceName", "When ACTIVE_STANDBY mode, determined each maxwell node name, formatted as —instanceName=<unique maxwell node name>" ).withRequiredArg();
		parser.accepts( "hostName", "When ACTIVE_STANDBY mode, determined node's network hostname, formatted as —hostName=<hostName>. default value is `InetAddress.getLocalHost().getHostName()`" ).withOptionalArg();
		parser.accepts( "clusterPort", "When ACTIVE_STANDBY mode, determined communication port for between nodes, formatted as —clusterPort=<using port>. default value is 12000" ).withOptionalArg();
		parser.accepts( "startController", "When ACTIVE_STANDBY mode, determined whether Helix Controller, formatted as startController=true|false. default value is true" ).withOptionalArg();

		return parser;
	}

	private void parse(String[] args) {
		OptionSet options = buildOptionParser().parse(args);
		Properties properties;
		String configFileName = DEFAULT_HA_CONFIG_FILE;
		Boolean abortOnMissing = false;

		if( options.has("haconfig") ) {
			configFileName = (String) options.valueOf("haconfig");
			abortOnMissing = true;
		}

		properties = readPropertiesFile(configFileName, abortOnMissing);
		if(properties == null) properties = new Properties();

		setup(options, properties);
	}

	private String fetchOption(String name, OptionSet options, Properties properties, String defaultVal) {
		if ( options != null && options.has(name) )
			return (String) options.valueOf(name);
		else if ( (properties != null) && properties.containsKey(name) )
			return (String) properties.getProperty(name);
		else
			return defaultVal;
	}

	private void setup(OptionSet options, Properties properties){
		this.zkAddress = fetchOption("zkAddress",options, properties, "localhost");
		this.clusterName = fetchOption("clusterName", options, properties, "maxwell");
		this.instanceName = fetchOption("instanceName", options, properties, "maxwell");
		this.hostName = fetchOption("hostName", options, properties, "localhost");
		this.clusterPort = fetchOption("clusterPort", options, properties, "");
		this.startController = Boolean.valueOf(fetchOption("startController", options, properties, "true"));
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

	public String getHostName() {
		return hostName;
	}

	public String getClusterPort() {
		return clusterPort;
	}

	public Boolean getStartController() {
		return startController;
	}
}
