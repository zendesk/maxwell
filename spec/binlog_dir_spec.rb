# encoding: ascii-8bit

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
    [].tap do |events|
      @binlog_dir.read_binlog(filter, start_at, end_at)  do |event|
        events << event
      end
    end
  end

  def map_to_hashes(events)
    events.map { |e| e.row_attributes_as_sql.to_a.map { |h| h.to_hash }  }.flatten
  end

  def hex_string(str)
    "x'" + str.each_byte.map { |b| b.to_s(16) }.join + "'"
  end

  describe "read_binlog" do
    before do
      @events = get_events({}, @start_position, get_master_position)
      @expected_row =
        { "id" => "1",
          "account_id" => "1",
          "nice_id" => "1",
          "status_id" => "2",
          "date_field" => "'1979-10-01 00:00:00'",
          "text_field" => "'Some Text'",
          "latin1_field" => hex_string("FooBar\xE4"),
          "utf8_field" => "'FooBarä'".force_encoding("utf-8"),
          "float_field" => "1.33",
          "timestamp_field" => "'1980-01-01 00:00:00'"
        }
    end

    it "yields some events" do
      @events.should_not be_empty
    end

    it "specifies the type of event" do
      @events.map(&:type).sort.uniq.should == ['delete', 'insert', 'update']
    end

    it "provides inserts" do
      inserts = @events.select { |e| e.type == 'insert' }
      map_to_hashes(inserts).should include(@expected_row)
    end

    it "provides updates" do
      updates = @events.select { |e| e.type == 'update' }
      map_to_hashes(updates).should include(@expected_row.merge("status_id" => '1', "text_field" => "'Updated Text'"))
    end

    it "provides deletes" do
      @events.map(&:to_sql).should include("DELETE FROM `sharded` WHERE id in (2)")
    end

    it "stops at the specified position" do
      position = get_master_position
      $mysql_master.connection.query("DELETE from sharded where id = 1")
      events = get_events({}, @start_position, position)
      e = events.detect { |e| e.type == :delete && e.attrs['id'] == 1 }
      e.should be_nil
    end

    it "returns the next position in the file" do
      position = get_master_position
      $mysql_master.connection.query("DELETE from sharded where id = 1")
      res = @binlog_dir.read_binlog({}, @start_position, position) {}

      res[:pos].should be >= position[:pos]
    end

    describe "BinglogEvent#to_sql" do
      before do
        @sql = @events.map(&:to_sql)
      end

      it "maps inserts into REPLACE statements" do
        @sql.should include(
          "REPLACE INTO `sharded` " +
          "(id, account_id, nice_id, status_id, date_field, text_field, latin1_field, utf8_field, float_field, timestamp_field) " +
          "VALUES (1,1,1,2,'1979-10-01 00:00:00','Some Text',#{hex_string("FooBar\xE4")},'FooBar\xC3\xA4',1.33,'1980-01-01 00:00:00')".force_encoding('utf-8')
        )
      end

      it "maps updates into REPLACE statements" do
        @sql.should include(
          "REPLACE INTO `sharded` " +
          "(id, account_id, nice_id, status_id, date_field, text_field, latin1_field, utf8_field, float_field, timestamp_field) " +
          "VALUES (1,1,1,1,'1979-10-01 00:00:00','Updated Text',#{hex_string("FooBar\xE4")},'FooBar\xC3\xA4',1.33,'1980-01-01 00:00:00')".force_encoding('utf-8')
        )
      end

      it "maps deletes into DELETE statements" do
        @sql.should include("DELETE FROM `sharded` WHERE id in (2)")
      end
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


