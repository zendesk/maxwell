require 'bundler/setup'
require 'mysql_isolated_server'
require 'debugger'
$LOAD_PATH << File.expand_path(File.dirname(__FILE__) + "/..")

def insert_row(table, attrs)
  array = attrs.to_a
  columns = array.map(&:first).join(",")
  values = array.map(&:last).map { |v|
    case v
    when String
      "'" + $mysql_master.connection.escape(v) + "'"
    when Fixnum
      v
    else
      "'" + $mysql_master.connection.escape(v.to_s) + "'"
    end
  }.join(',')
  $mysql_master.connection.query("INSERT INTO #{table} (#{columns}) values(#{values})")
end


def generate_binlog_events
  insert_row('sharded',
    account_id: 1,
    nice_id: 1,
    status_id: 2,
    date_field: Time.now,
    text_field: "Some Text",
    latin1_field: "FooBarä".encode('ISO-8859-1'),
    utf8_field: "FooBarä",
    float_field:  1.33333333333,
  )

  insert_row('sharded',
    account_id: 1,
    nice_id: 2,
    status_id: 2,
    date_field: Time.now,
    text_field: "Delete Me",
    latin1_field: "FooBarä".encode('ISO-8859-1'),
    utf8_field: "FooBarä",
    float_field:  1.33333333333,
  )

  $mysql_master.connection.query("FLUSH LOGS")

  $mysql_master.connection.query("UPDATE sharded set status_id = 1")
  $mysql_master.connection.query("DELETE FROM sharded where nice_id = 1")
end

def get_master_position
  res = $mysql_master.connection.query("SHOW MASTER STATUS")
  row = res.first
  {file: row['File'], pos: row['Position']}
end

$mysql_master = MysqlIsolatedServer.new
$mysql_binlog_dir = $mysql_master.base + "/binlogs"
Dir.mkdir($mysql_binlog_dir)

$mysql_master.params = "--log-bin=#{$mysql_binlog_dir}/master --binlog_format=row"
$mysql_master.boot!

$mysql_initial_binlog_pos = get_master_position

$mysql_master.connection.query "CREATE DATABASE shard_1"

$mysql_master.connection.query "USE shard_1"

Dir.glob(File.dirname(__FILE__) + "/sql/*.sql").each do |fname|
  $mysql_master.connection.query File.read(fname)
end


generate_binlog_events

