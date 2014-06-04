# encoding: ascii-8bit
#
require_relative 'helper'

require 'lib/binlog_dir'
require 'lib/binlog_event'
require 'lib/schema'

describe 'BinlogEvent' do
  before do
    reset_master
    @schema = Schema.new($mysql_master.connection, "shard_1")
    @schema.fetch
    @binlog_dir = BinlogDir.new($mysql_binlog_dir, @schema)
  end

  def capture_binlog_events(sql)
    @start_position = @schema.binlog_info
    $mysql_master.connection.query(sql)
    [].tap do |events|
      @binlog_dir.read_binlog({}, @start_position, nil)  do |event|
        events << event
      end
    end
  end

  describe "to_sql" do
    it "escapes tick characters" do
      events = capture_binlog_events("INSERT INTO minimal set account_id = 1, text_field = '\\'tick'")
      events.first.to_sql.should == "REPLACE INTO `minimal` (id, account_id, text_field) VALUES (1, 1, '\\'tick')"
    end

    it "deals with newlines" do
      events = capture_binlog_events("INSERT INTO minimal set account_id = 1, text_field = '\nfooo\n'")
      events.first.to_sql.should == "REPLACE INTO `minimal` (id, account_id, text_field) VALUES (1, 1, '\\nfooo\\n')"
    end
  end
end
