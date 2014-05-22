require 'mysql_binlog'

class NoSuchFileError < StandardError ; end

class BinlogDir
  def initialize(dir)
    @dir = dir
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
    binlog.each_event do |event|
      return if max_events && event_count > max_events
      next if event[:filename] == from_file && event[:position] < from_pos
      puts event.inspect
      yield event
      event_count += 1
    end
  end
end
