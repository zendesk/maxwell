package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.MaxwellConfig;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.Map;
import java.util.Properties;

/**
 * Created by springloops on 2016. 8. 31..
 */
public class HAMaxwellConfig extends MaxwellConfig {

	private static final String DEFAULT_HA_CONFIG_FILE = "ha.config.properties";

    public String zkAddress;
	public String clusterName;
	public String instanceName;
	public String hostName;
	public String clusterPort;
	public Boolean startController;

	public HAMaxwellConfig(String[] args) {
		super(args);
		this.parse(args);
		//this.validate();
	}

	@Override
	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();

		parser.accepts( "zkAddress", "When ACTIVE_STANDBY mode, determined zookeeper address, formatted as —zkAddress=zkIP:port[,skip:port]…" ).withOptionalArg();
		parser.accepts( "clusterName", "When ACTIVE_STANDBY mode, determined maxwell cluster name, formatted as —clusterName=<ClusterName>" ).withOptionalArg();
		parser.accepts( "instanceName", "When ACTIVE_STANDBY mode, determined each maxwell node name, formatted as —instanceName=<unique maxwell node name>" ).withOptionalArg();
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

		if( options.has("config") ) {
			configFileName = (String) options.valueOf("config");
			abortOnMissing = true;
		}

		properties = readPropertiesFile(configFileName, abortOnMissing);
		if(properties == null) properties = new Properties();

		setup(options, properties);
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
}
