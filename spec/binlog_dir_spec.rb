require_relative 'helper'
require 'lib/binlog_dir'
require 'lib/schema'

describe "BinlogDir" do
  before do
    reset_master
    @schema = Schema.new($mysql_master.connection, "shard_1")
    @schema.fetch
    @start_position = @schema.binlog_info
    @binlog_dir = BinlogDir.new($mysql_binlog_dir, @schema)
    generate_binlog_events
  end

  def get_events(filter, start_at, end_at)
    @events = []
    @binlog_dir.read_binlog(filter, start_at, end_at)  do |event|
      @events << event
    end
    @events.flatten!
  end
  describe "read_binlog" do
    before do
      get_events({}, @start_position, get_master_position)
    end

    it "yields some events" do
      @events.should_not be_empty
    end

    it "specifies the type of event" do
      @events.map { |e| e[:type] }.sort.uniq.should == ['delete', 'insert', 'update']
    end

    expected_row =
      { "id" => 1,
        "account_id" => 1,
        "nice_id" => 1,
        "status_id" => 2,
        "date_field" => "1979-10-01 00:00:00",
        "text_field" => "Some Text",
        "latin1_field" => "FooBar\xE4".force_encoding("ASCII-8BIT"),
        "utf8_field" => "FooBar\xC3\xA4".force_encoding("ASCII-8BIT"),
        "float_field" => 1.3300000429153442,
        "timestamp_field" => 315561600
      }
    it "provides the full row for inserts" do
      expected = {:type => 'insert', :row => expected_row}

      @events.should include(expected)
    end

    it "provides the full row for updates" do
      expected = {:type => 'update', :row => expected_row.merge("status_id" => 1)}
      @events.should include(expected)
    end

    it "provides the id for deletes" do
      @events.should include(:type => 'delete', :id => 2)
    end

    it "stops at the specified position" do
      position = get_master_position
      $mysql_master.connection.query("DELETE from sharded where id = 1")
      get_events({}, @start_position, position).should_not include(:type => 'delete', :id => 1)
    end

    it "returns the next position in the file" do
      position = get_master_position
      $mysql_master.connection.query("DELETE from sharded where id = 1")
      res = @binlog_dir.read_binlog({}, @start_position, position) {}

      res[:pos].should be >= position[:pos]
    end
  end

  describe "when a column is added mid-stream" do
    before do
      $mysql_master.connection.query("ALTER TABLE sharded add column unexpected int(11)")
      insert_row("sharded", account_id: 1, nice_id: 3)
    end

    it "should crash" do
      expect { @binlog_dir.read_binlog({}, @start_position, get_master_position) }.to raise_error
    end
  end

  describe "what happens when a column is dropped and then added?" do
    before do
      $mysql_master.connection.query("ALTER TABLE sharded add column unexpected int(11), drop column status_id")
      insert_row("sharded", account_id: 1, nice_id: 3)
    end

    it "should crash" do
      expect { @binlog_dir.read_binlog({}, @start_position, get_master_position) {} }.to raise_error
    end

  end
end


