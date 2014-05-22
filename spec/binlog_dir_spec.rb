require_relative 'helper'
require 'lib/binlog_dir'
require 'lib/schema'

describe "BinlogDir" do
  before do
    @schema = Schema.new($mysql_master.connection, "shard_1")
    @binlog_dir = BinlogDir.new($mysql_binlog_dir, @schema)
  end

  describe "read_binlog" do
    it "yields some events" do
      events = []
      @binlog_dir.read_binlog({}, $mysql_initial_binlog_pos, get_master_position) do |event|
        events << event
      end
      events.should_not be_empty
      puts events
    end
  end
end


