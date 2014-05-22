require 'mysql_binlog'

class NoSuchFileError < StandardError ; end

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

  def reformat_row_event(e)
    ev = e[:event]
    table = ev[:table]
    columns = @schema.fetch[table[:table]]
    raise "Table #{table[:table]} not found in schema!" unless columns
    ev[:row_image].map do |h|
      image = (e[:type] == :delete_rows_event) ? h[:before] : h[:after]
      image.inject({}) do |accum, c|
        idx = c.keys.first
        val = c.values.first
        accum[columns[idx][:column_name]] = val
        accum
      end
    end
  end
end
