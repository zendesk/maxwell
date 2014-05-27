require 'mysql_binlog'

class NoSuchFileError < StandardError ; end
class SchemaChangedError < StandardError ; end

class BinlogDir
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

    binlog.each_event do |event|
      return if max_events && event_count > max_events
      next if event[:filename] == from_file && event[:position] < from_pos

      if [:write_rows_event, :update_rows_event, :delete_rows_event].include?(event[:type])
        next unless event[:event][:table][:db] == @schema.db

        yield reformat_row_event(event)
      end
      event_count += 1
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

  def reformat_row_event(e)
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
      case e[:type]
      when :write_rows_event
        {:type => 'insert', :row => row}
      when :update_rows_event
        {:type => 'update', :row => row}
      when :delete_rows_event
        {:type => 'delete', :id => row['id']}
      end
    end
  end
end
