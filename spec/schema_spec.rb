require_relative 'helper'
require 'lib/schema'

describe "Schema" do
  before do
    @schema = Schema.new($mysql_master.connection, "shard_1")
  end

  describe "fetch" do
    it "reads the schema" do
      schema = @schema.fetch

      schema["sharded"].should_not be_nil
      schema["sharded"].first.should == {:column_name=>"id", :ordinal_position=>1, :column_default=>nil, :is_nullable=>"NO",
                              :data_type=>"bigint", :character_maximum_length=>nil, :character_octet_length=>nil, :numeric_precision=>19,
                              :numeric_scale=>0, :character_set_name=>nil, :collation_name=>nil, :column_type=>"bigint(20)"}
    end
  end

  describe "save" do
    it "dumps the schema to a file" do
      @schema.save
    end
  end

  describe "load" do
    before { @schema.save }
    it "loads the dumped schema" do
      new_schema = Schema.load(@schema.filename)
      new_schema.fetch.should == @schema.fetch
    end

    it "keeps a small cache" do
      new_schema = Schema.load(@schema.filename)
      new_schema.fetch.should == @schema.fetch

      File.unlink(DATA_DIR + "/" + @schema.filename)
      cached_schema = Schema.load(@schema.filename)
      cached_schema.should == new_schema
    end
  end
end
