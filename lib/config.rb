require 'lib/jdbc_connection'

class BinlogConfig
  BINLOG_SERVICE_CONFIG = File.join(File.dirname(__FILE__) + "/../config/exodus_binlog_service.yml")

  def config
    @config ||= YAML.load_file(BINLOG_SERVICE_CONFIG)
  end
  def binlog_dir
    config['binlog_dir']
  end

  def mysql_connection
    @mysql_connection ||= WrappedJDBCConnection.new(username: config['mysql']['username'], password: config['mysql']['password'])
  end

  def api_port
    config['port']
  end
end
