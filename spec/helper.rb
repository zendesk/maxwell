# encoding: utf-8
require 'bundler/setup'
require_relative "../lib/setup_java"

require 'mysql_isolated_server'
require 'ruby-debug'

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
    when Time
      "'" + v.strftime("%Y-%m-%d %H:%M:%S") + "'"
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
    date_field: Time.parse("1979-10-01"),
    text_field: "Some Text",
    latin1_field: "FooBarä".encode('ISO-8859-1'),
    utf8_field: "FooBarä",
    float_field:  1.33333333333,
    timestamp_field: Time.parse("1980-01-01"),
    decimal_field: 8.621
  )

  insert_row('sharded',
    account_id: 1,
    nice_id: 2,
    status_id: 2,
    date_field: Time.parse("1979-10-01"),
    text_field: "Delete Me",
    latin1_field: "FooBarä".encode('ISO-8859-1'),
    utf8_field: "FooBarä",
    float_field:  1.33333333333,
    timestamp_field: Time.parse("1980-01-01"),
    decimal_field: 8.621
  )

  $mysql_master.connection.query("FLUSH LOGS")

  $mysql_master.connection.query("UPDATE sharded set status_id = 1, text_field = 'Updated Text', timestamp_field=timestamp_field where id = 1")
  $mysql_master.connection.query("DELETE FROM sharded where nice_id = 2")
end

def reset_master
  $mysql_master.connection.query "DROP DATABASE if exists shard_1"
  $mysql_master.connection.query "CREATE DATABASE shard_1"

  $mysql_master.connection.query "USE shard_1"

  Dir.glob(File.dirname(__FILE__) + "/sql/*.sql").each do |fname|
    $mysql_master.connection.query File.read(fname)
  end

  $mysql_master.connection.query("RESET MASTER")
end

def get_master_position
  res = $mysql_master.connection.query("SHOW MASTER STATUS")
  row = res.first
  {file: row['File'], pos: row['Position']}
end

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end

Thread.abort_on_exception = true
$mysql_master = MysqlIsolatedServer.thread_boot("--log-bin=binlogs/master", "--", "--binlog_format=row")
$mysql_initial_binlog_pos = get_master_position
$mysql_binlog_dir = $mysql_master.base + "/mysqld/binlogs"

$mysql_master.connection.query "CREATE DATABASE shard_1"

$mysql_master.connection.query "USE shard_1"

Dir.glob(File.dirname(__FILE__) + "/sql/*.sql").each do |fname|
  $mysql_master.connection.query File.read(fname)
end

class Object
  def my_methods
    (methods - Object.methods).sort
  end
end

generate_binlog_events

