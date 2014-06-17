require_relative 'helper'
require 'lib/binlog_dir'

describe "BinlogDir" do
  before do
    @binlog_dir = BinlogDir.new($mysql_binlog_dir)
  end

  describe "read_binlog" do
    it "yields some events" do
      events = []
      puts get_master_position
      @binlog_dir.read_binlog({}, $mysql_initial_binlog_pos, get_master_position) do |event|
        events << event
      end
      events.should_not be_empty
    end
  end
end


