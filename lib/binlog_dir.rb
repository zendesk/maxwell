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
  java_import 'com.zendesk.exodus.ExodusAbstractRowsEvent'
  java_import 'com.google.code.or.common.util.MySQLConstants'

  COLUMN_TYPES = MySQLConstants.constants.inject({}) do |h, c|
    c = c.to_s
    next h unless c.to_s =~ /^TYPE_(.*)$/
    val = MySQLConstants.const_get(c)
    h[val] = $1.downcase.to_sym
    h
  end

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
    # TODO stop positions

    event_count = 0
    # this is the schema that was fetched at the top of the entire process -- the "binlog start position"
    column_hash = @schema.fetch
    table_map_cache = {}

    next_position = {file: from_file, pos: from_pos}

    while e = parser.getEvent()
      next_position = {file: e.binlog_filename, pos: e.header.next_position}

      case e.header.event_type
      when MySQLConstants::TABLE_MAP_EVENT
        if table_map_cache[e.table_id]
          #raise "table map changed!" if table_map_event_to_hash(e, filter) != table_map_cache[e.table_id]
        else
          next if e.database_name.to_string != @schema.db
          table_map_cache[e.table_id] = table_map_event_to_hash(e)
        end
      when MySQLConstants::WRITE_ROWS_EVENT, MySQLConstants::UPDATE_ROWS_EVENT, MySQLConstants::DELETE_ROWS_EVENT
        event_count += 1

        table_schema = table_map_cache[e.table_id]
        next unless table_schema
        next if exclude_tables.include?(table_schema[:name])
        yield ExodusAbstractRowsEvent.build_event(e, *table_schema.values_at(:name, :column_names, :column_encodings, :id_offset))
      end
      break if max_events && event_count >= max_events
    end

    parser.stop()
    next_position[:processed] = event_count
    next_position
  end


  def table_map_event_to_hash(e)
    md = e.get_column_metadata

    h = {}
    h[:name] = e.table_name.to_string
    h[:database] = e.database_name.to_string
    h[:columns] = []

    captured_schema = @schema.fetch[h[:name]]

    if captured_schema.nil?
      raise SchemaChangedError.new("Could not find #{h[:name]} in stored schema!")
    end

    if e.column_types.size != captured_schema.size
      msg = "expected #{captured_schema.size} columns in #{h[:name]} but got #{e.column_types.size} columns"
      raise SchemaChangedError.new(msg)
    end

    e.column_types.each_with_index do |type, i|
      column = {}
      type = 256 + type if type < 0
      column[:type] = COLUMN_TYPES[type]
      h[:columns] << {
        :type => column[:type],
        :metadata => md.metadata(i),
        :position => i,
        :name => captured_schema[i][:column_name],
        :character_set => captured_schema[i][:character_set_name]
      }
    end

    verify_table_schema!(captured_schema, h)

    h[:id_offset] = h[:columns].find_index { |c| c[:column_key] == 'PRI' }
    h[:column_names] = h[:columns].map { |c| c[:name] }
    h[:column_encodings]  = h[:columns].map { |c| c[:character_set] }
    h
  end

  def verify_table_schema!(expected_columns, table)
    name, tcolumns = table.values_at(:name, :columns)


    expected_columns.zip(tcolumns) do |c, t|
      eq = case c[:data_type]
      when 'bigint'
        t[:type] == :longlong
      when 'int'
        t[:type] == :long
      when 'smallint'
        t[:type] == :short
      when 'tinyint'
        t[:type] == :tiny
      when 'datetime'
        t[:type] == :datetime
      when 'text', 'mediumtext'
        t[:type] == :blob
      when 'varchar', 'float', 'timestamp'
        t[:type].to_s == c[:data_type]
      when 'date'
        t[:type] == :date
      when 'decimal'
        t[:type] == :decimal || t[:type] == :newdecimal
      else
        raise "unknown columns type '#{c[:data_type]}' (#{t[:type]})?"
      end

      if !eq
        msg = "in `#{table[:name]}`, expected column ##{c[:ordinal_position]} (`#{c[:column_name]}`?) to be a #{c[:data_type]}, "
        msg += "instead saw a #{t[:type]}"
        raise SchemaChangedError.new(msg)
      end
    end
    true
  end
end
