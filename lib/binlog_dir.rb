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

  java_import 'com.zendesk.exodus.ExodusParser'
  java_import 'com.zendesk.exodus.ExodusRowFilter'

  def read_binlog(from, to, options = {}, &block)
    from_file = from.fetch(:file)
    from_pos  = from.fetch(:pos)
    raise_unless_exists(from_file)

    if to
      to_file   = to.fetch(:file)
      to_pos    = to.fetch(:pos)
      raise_unless_exists(to_file)
    end

    max_events = options[:max_events]
    exclude_tables = options[:exclude_tables] || []

    parser = ExodusParser.new(@dir, from_file)
    parser.start_position = from_pos

    filter = ExodusRowFilter.new


    # this is the schema that was fetched at the top of the entire process -- the "binlog start position"
    parser.schema = @schema

    event_count = 0

    next_position = {file: from_file, pos: from_pos}

    stop_parser = false
    while e = parser.getEvent(stop_parser)
      stop_parser = (max_events && parser.row_events_processed >= max_events)

      yield e

      next_position = {file: e.binlog_filename, pos: e.header.next_position}
    end

    parser.stop()
    next_position[:processed] = event_count
    next_position
  end
end
