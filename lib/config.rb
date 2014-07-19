require 'lib/jdbc_connection'

class BinlogConfig
  BINLOG_SERVICE_CONFIG = File.join(File.dirname(__FILE__) + "/../config/exodus_binlog_service.yml")
  def initialize(config_file = nil)
    @config_file = (config_file || BINLOG_SERVICE_CONFIG)
  end

  def load_config!
    YAML.load_file(@config_file)
  end

  def config
    @config ||= load_config!
  end

  def config=(config)
    @config = config
  end

  def binlog_dir
    config['binlog_dir']
  end

  def mysql_connection
    WrappedJDBCConnection.new(username: config['mysql']['username'],
                              password: config['mysql']['password'],
                              host: config['mysql']['host'],
                              port: config['mysql']['port'])
  end

  def api_port
    config['port']
  end
end
