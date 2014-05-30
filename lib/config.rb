require 'mysql2'
class BinlogConfig
  def binlog_dir
    "/opt/local/var/db/mysql5"
  end

  def mysql_connection
    @mysql_connection ||= Mysql2::Client.new(username: 'root')
  end
end
