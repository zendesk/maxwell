require_relative 'helper'
require 'lib/binlog_dir'

describe "BinlogDir" do
  before do
    @binlog_dir = BinlogDir.new($mysql_binlog_dir)
  end
end


