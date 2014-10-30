require 'yaml'
require 'monitor'

DATA_DIR=File.expand_path(File.dirname(__FILE__) + "/../data")

class Schema
  attr_reader :db

  def initialize(connection, db_name, schema=nil)
    @db = db_name
    @cx = connection
    @schema = schema
  end

  def fetch
    return @schema if @schema

    @schema = {}

    binlog_info

    tables = @cx.query("show tables from #{@db}")
    tables.each do |row|
      table = row.values.first
      @schema[table] = []

      cols = @cx.query("select * from information_schema.columns where TABLE_SCHEMA='#{@db}' and TABLE_NAME='#{table}' ORDER BY ORDINAL_POSITION")
      cols.each do |col|
        c = col.inject({}) do |accum, array|
          k, v = *array
          k = k.downcase.to_sym
          next accum if [:table_catalog, :table_schema, :table_name, :column_key, :extra, :privileges, :column_comment, :column_key].include?(k)
          accum[k] = v
          accum
        end
        @schema[table] << c
      end
    end
    @schema
  end

  def binlog_info
    @binlog_info ||= begin
      res = @cx.query("SHOW MASTER STATUS")
      row = res.first
      {file: row['File'], pos: row['Position']}
    end
  end

  def save
    fetch

    File.open(fullpath, "w+") do |f|
      f.write(@schema.to_yaml)
    end
  end

  def filename
    self.class.schema_fname(@db, @binlog_info[:file], @binlog_info[:pos])
  end

  def fullpath
    DATA_DIR + "/" + filename
  end

  def self.schema_fname(db, logfile, pos)
    [db, logfile, pos].join('--') + ".yaml"
  end

  @schema_cache = {}
  @schema_cache.extend(MonitorMixin)
  MAX_SCHEMAS = 15

  def self.memory_cache(token)
    @schema_cache.synchronize do
      if @schema_cache[token]
        return @schema_cache[token]
      else
        @schema_cache[token] = yield
        @schema_cache.delete(@schema_cache.keys.first) while @schema_cache.size > MAX_SCHEMAS
        return @schema_cache[token]
      end
    end
  end

  def self.load(token)
    memory_cache(token) do
      path = DATA_DIR + "/" + token
      db, logfile, pos = token.split("--")

      return nil unless File.exist?(path)
      schema = YAML.load(File.read(path))
      new(nil, db, schema)
    end
  end
end
