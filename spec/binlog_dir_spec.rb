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

  def get_events(start_at, end_at, options = {}, output = {})
    [].tap do |events|
      ret = @binlog_dir.read_binlog(start_at, end_at, options)  do |event|
        events << event
      end
      output.merge!(ret)
    end
  end

  def events_for(options = {}, &block)
    start_pos = get_master_position
    yield
    get_events(start_pos, get_master_position, options)
  end

  def map_to_hashes(events)
    events.map { |e| e.row_attributes_as_sql.to_a.map { |h| h.to_hash }  }.flatten
  end

  def hex_string(str)
    "x'" + str.each_byte.map { |b| b.to_s(16) }.join + "'"
  end

  def stringify_event(h)
    h.keys.each do |key|
      h[key.to_s] = h.delete(key).to_s
    end
    h
  end

  describe "read_binlog" do
    before do
      @events = get_events(@start_position, get_master_position)
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
          "timestamp_field" => "'1980-01-01 00:00:00'",
          "decimal_field" => "8.6210000"
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
      stop_position = get_master_position

      # is after stop_position, should not be included
      $mysql_master.connection.query("DELETE from sharded where id = 1")

      events = get_events(@start_position, stop_position)
      e = events.detect { |e| e.type == :delete && e.attrs['id'] == 1 }
      e.should be_nil
    end

    it "returns the next position in the file" do
      position = get_master_position
      $mysql_master.connection.query("DELETE from sharded where id = 1")
      res = @binlog_dir.read_binlog(@start_position, position) {}

      res[:pos].should be >= position[:pos]
    end

    it "stops after processing max_events" do
      @events = get_events(@start_position, get_master_position, max_events: 1)
      @events.size.should == 1
    end

    it "only stops on a table_map_event" do
      output = {}
      start_pos = get_master_position

      insert_row('minimal', account_id: 1, text_field: 'a')
      # trigger the bug we're testing by having the event limit get reached on an ignored row
      insert_row('mediumints', account_id: 1, medium: 2)
      insert_row('minimal', account_id: 1, text_field: 'b')

      events = get_events(start_pos, get_master_position, {max_events: 2, exclude_tables: ['mediumints']}, output)
      events.size.should == 1
      output[:processed].should == 2

      events = get_events(output, get_master_position, {max_events: 1, exclude_tables: ['mediumints']}, output)
      events.size.should == 1
    end

    describe "BinglogEvent#to_sql" do
      before do
        @sql = @events.map(&:to_sql)
      end

      it "maps inserts into REPLACE statements" do
        @sql.should include(
          "REPLACE INTO `sharded` " +
          "(`id`, `account_id`, `nice_id`, `status_id`, `date_field`, `text_field`, `latin1_field`, `utf8_field`, `float_field`, `timestamp_field`, `decimal_field`) " +
          "VALUES (1,1,1,2,'1979-10-01 00:00:00','Some Text',#{hex_string("FooBar\xE4")},'FooBar\xC3\xA4',1.33,'1980-01-01 00:00:00',8.6210000)".force_encoding('utf-8')
        )
      end

      it "maps updates into REPLACE statements" do
        @sql.should include(
          "REPLACE INTO `sharded` " +
          "(`id`, `account_id`, `nice_id`, `status_id`, `date_field`, `text_field`, `latin1_field`, `utf8_field`, `float_field`, `timestamp_field`, `decimal_field`) " +
          "VALUES (1,1,1,1,'1979-10-01 00:00:00','Updated Text',#{hex_string("FooBar\xE4")},'FooBar\xC3\xA4',1.33,'1980-01-01 00:00:00',8.6210000)".force_encoding('utf-8')
        )
      end

      it "maps deletes into DELETE statements" do
        @sql.should include("DELETE FROM `sharded` WHERE id in (2)")
      end

      it "filters out rows that don't match the filter" do
        insert_row('sharded',
          account_id: 2,
          nice_id: 2,
          status_id: 2,
          date_field: Time.parse("1979-10-01"),
          text_field: "Filtered row",
          latin1_field: "FooBar".force_encoding('ISO-8859-1'),
          utf8_field: "FooBar".encode('utf-8'),
          float_field:  1.33333333333,
          timestamp_field: Time.parse("1980-01-01")
        )
        events = get_events(@start_position, get_master_position)
        events.map { |r| r.to_sql('account_id' => 2) }.compact.size.should == 1
      end

      it "puts multiple statements on the same line" do
        events = events_for do
          $mysql_master.connection.query("insert into minimal (account_id, text_field) VALUES (1, 'a'), (1, 'b')")
        end
        e = events.first
        e.to_sql.should_not include("\n")
      end

      it "deals with mediumints" do
        events = events_for do
          insert_row('mediumints',
            account_id: 1,
            medium: 2,
          )
        end
        map_to_hashes(events).should eq([{"id" =>  "1", "account_id" => "1", "medium" => "2"}])
      end

      context "unsigned integers" do
        it "interprets it properly" do
          row = {
              i64: (2**64) - 1,
              i32: (2**32) - 1,
              i24: (2**24) - 1,
              i16: (2**16) - 1,
              i8:  (2**8) - 1
          }
          events = events_for do
            insert_row('ints', row)
          end
          map_to_hashes(events).should eq([stringify_event(row).merge("id" => "1")])
        end
      end
    end
  end

  describe "when a column is added mid-stream" do
    before do
      $mysql_master.connection.query("ALTER TABLE sharded add column unexpected int(11)")
      insert_row("sharded", account_id: 1, nice_id: 3)
    end

    it "should crash" do
      expect { @binlog_dir.read_binlog(@start_position, get_master_position) {} }.to raise_error(SchemaChangedError)
    end
  end

  describe "when given an EOF master binlog position" do
    before do
      @pos = get_master_position
      @ret = @binlog_dir.read_binlog(@pos, @pos) {}
    end

    it "returns the input position" do
      expect(@ret).to eq(@pos.merge(:processed => 0))
    end
  end

  describe "what happens when a column is dropped and then added?" do
    before do
      $mysql_master.connection.query("ALTER TABLE sharded add column unexpected int(11), drop column status_id")
      insert_row("sharded", account_id: 1, nice_id: 3)
    end

    it "should crash" do
      expect { @binlog_dir.read_binlog(@start_position, get_master_position) {} }.to raise_error(SchemaChangedError)
    end
  end

  describe "when a table is added mid-stream" do
    before do
      $mysql_master.connection.query("CREATE TABLE surprise ( id int(10) PRIMARY KEY AUTO_INCREMENT, account_id int(10) )")
      insert_row("surprise", account_id: 1)
    end

    after do
      $mysql_master.connection.query("DROP TABLE surprise")
    end

    it "should crash" do
      expect { @binlog_dir.read_binlog(@start_position, get_master_position) {} }.to raise_error(SchemaChangedError)
    end
  end

  describe "exclude_tables" do
    it "should not include excluded tables" do
      events = events_for(exclude_tables: ['minimal']) do
        $mysql_master.connection.query("INSERT INTO minimal set id = 12, account_id = 123")
      end
      expect(events.map(&:to_sql).join("\n")).to_not match(/minimal/)
    end
  end
end


