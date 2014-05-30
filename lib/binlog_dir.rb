require 'mysql_binlog'
require 'lib/binlog_event'

class NoSuchFileError < StandardError ; end
class SchemaChangedError < StandardError ; end

class BinlogDir
  attr_reader :next_position

  def initialize(dir, schema)
    @dir = dir
    @schema = schema
  end

  def exists?(file)
    File.exists?(File.join(@dir, file))
  end

  def raise_unless_exists(file)
    raise NoSuchFileError.new("#{@dir}/#{file} not found!") unless exists?(file)
  end

  def read_binlog(filter, from, to, max_events=nil)
    from_file = from.fetch(:file)
    from_pos  = from.fetch(:pos)
    to_file   = to.fetch(:file)
    to_pos    = to.fetch(:pos)

    raise_unless_exists(from_file)
    raise_unless_exists(to_file)
    binlog = MysqlBinlog::Binlog.new(MysqlBinlog::BinlogFileReader.new(File.join(@dir, from_file)))

    event_count = 0
    column_hash = @schema.fetch

    next_position = nil
    binlog.each_event do |event|
      next_position = {file: event[:filename], pos: event[:header][:next_position]}
      break if event[:filename] == to_file && event[:position] >= to_pos
      break if max_events && event_count > max_events

      next if event[:filename] == from_file && event[:position] < from_pos

      if [:write_rows_event, :update_rows_event, :delete_rows_event].include?(event[:type])
        next unless event[:event][:table][:db] == @schema.db

        row_events = reformat_binlog_event(event)
        row_events.each do |r|
          next unless attrs_match_filter?(r[:row], filter)

          yield(BinlogEvent.new(r[:type], r[:row]))
          event_count += 1
        end
      end
    end

    next_position
  end

  def attrs_match_filter?(attrs, filter)
    filter.all? do |key, value|
      attrs[key] == value
    end
  end

  def verify_table_schema!(columns, table)
    tcolumns = table[:columns]
    if columns.size != tcolumns.size
      raise SchemaChangedError.new("expected #{columns.size} columns but got #{tcolumns.size} columns")
    end

    columns.zip(tcolumns) do |c, t|
      eq = case c[:data_type]
      when 'bigint'
        t[:type] == :longlong
      when 'int'
        t[:type] == :long
      when 'tinyint'
        t[:type] == :tiny
      when 'datetime'
        t[:type] == :datetime
      when 'text'
        t[:type] == :blob
      when 'varchar', 'float', 'timestamp'
        t[:type].to_s == c[:data_type]
      else
        raise "unknown columns type #{c[:data_type]}"
      end

      if !eq
        msg = "expected column #{c[:ordinal_position]} (#{c[:column_name]})? to be a #{c[:data_type]}, "
        msg += "instead saw a #{t[:type]}"
        raise SchemaChangedError.new(msg)
      end
    end
    true
  end

  def reformat_binlog_event(e)
    ev = e[:event]
    table = ev[:table]
    columns = @schema.fetch[table[:table]]

    raise SchemaChangeError.new("Table #{table[:table]} not found in schema!") unless columns
    verify_table_schema!(columns, table)


    ev[:row_image].map do |h|
      image = (e[:type] == :delete_rows_event) ? h[:before] : h[:after]

      row = image.inject({}) do |accum, c|
        idx = c.keys.first
        val = c.values.first
        accum[columns[idx][:column_name]] = val
        accum
      end
      type = case e[:type]
      when :write_rows_event
        'insert'
      when :update_rows_event
        'update'
      when :delete_rows_event
        'delete'
      end
      {:type => type, :row => row}
    end
  end
end
