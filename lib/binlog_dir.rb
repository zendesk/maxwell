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

  # read an event from the binlog, transforming into a BinlogEvent.  Here's what a sample row looks like.

  java_import 'com.zendesk.exodus.ExodusParser'
  java_import 'com.google.code.or.common.util.MySQLConstants'
  java_import 'java.text.SimpleDateFormat'
  java_import 'com.zendesk.exodus.ExodusAbstractRowsEvent'

  COLUMN_TYPES = MySQLConstants.constants.inject({}) do |h, c|
    c = c.to_s
    next h unless c.to_s =~ /^TYPE_(.*)$/
    val = MySQLConstants.const_get(c)
    h[val] = $1.downcase.to_sym
    h
  end

  EVENT_TYPES = {
    MySQLConstants::WRITE_ROWS_EVENT => :insert,
    MySQLConstants::UPDATE_ROWS_EVENT => :update,
    MySQLConstants::DELETE_ROWS_EVENT => :delete
  }

  def read_binlog(filter, from, to, max_rows=nil, &block)
    filter = filter.inject({}) do |h, arr|
      k, v = *arr
      h[k.to_s] = v
      h
    end

    from_file = from.fetch(:file)
    from_pos  = from.fetch(:pos)
    raise_unless_exists(from_file)

    if to
      to_file   = to.fetch(:file)
      to_pos    = to.fetch(:pos)
      raise_unless_exists(to_file)
    end

    parser = ExodusParser.new(@dir, from_file)
    parser.start_position = from_pos
    # TODO stop positions

    row_count = 0
    # this is the schema that was fetched at the top of the entire process -- the "binlog start position"
    column_hash = @schema.fetch
    next_position = nil
    table_map_cache = {}

    while e = parser.getEvent()
      next_position = {file: "foo", pos: e.header.next_position}

      case e.header.event_type
      when MySQLConstants::TABLE_MAP_EVENT
        if table_map_cache[e.table_id]
          #raise "table map changed!" if table_map_event_to_hash(e, filter) != table_map_cache[e.table_id]
        else
          next if e.database_name.to_string != @schema.db
          table_map_cache[e.table_id] = table_map_event_to_hash(e, filter)
        end
      when MySQLConstants::WRITE_ROWS_EVENT, MySQLConstants::UPDATE_ROWS_EVENT, MySQLConstants::DELETE_ROWS_EVENT
        table_schema = table_map_cache[e.table_id]
        next unless table_schema
        yield ExodusAbstractRowsEvent.build_event(e, table_schema[:name], table_schema[:column_names], table_schema[:id_offset])
      end
    end
    break if max_rows && row_count > max_rows
    next_position
  end

  def date_format(date)
    return nil if date.nil?
    @df ||= SimpleDateFormat.new("yyyy-MM-dd HH:mm:ss")
    @df.format(date)
  end

  def reformat_binlog_event(e, schema, &block)
    return [] if schema[:filter] && schema[:filter] == :reject

    res = []
    e.rows.each do |r|
      if e.header.event_type == MySQLConstants::UPDATE_ROWS_EVENT
        r = r.after # we don't care about the past.  livin' for today.
      end

      if schema[:filter]
        next unless schema[:filter].all? do |pos, val|
          r.columns[pos].value == val
        end
      end

      attrs = {}
      r.columns.each_with_index do |c, i|
        schema_column = schema[:columns][i]

        if c.value.nil?
          attrs[schema_column[:name]] = nil
          next
        end

        val = case schema_column[:type]
        when :tiny, :short, :long, :longlong, :int24, :float
          c.value
        when :tiny_blob, :medium_blob, :long_blob, :varchar
          c.value.to_s
        when :datetime, :timestamp
          date_format(c.value)
        else
          debugger
          raise schema[:columns][i].inspect + " not supported!"
        end
        attrs[schema_column[:name]] = val
      end

      res << attrs
    end
    res
  end

  def attrs_match_filter?(attrs, filter)
    filter.all? do |key, value|
      attrs[key] == value
    end
  end

  def table_map_event_to_hash(e, filter)
    md = e.get_column_metadata

    h = {}
    h[:name] = e.table_name.to_string
    h[:database] = e.database_name.to_string
    h[:columns] = []

    captured_schema = @schema.fetch[h[:name]]

    e.column_types.each_with_index do |type, i|
      column = {}
      type = 255 + type if type < 0
      column[:type] = COLUMN_TYPES[type]
      h[:columns] << {:type => column[:type], :metadata => md.metadata(i), :position => i, :name => captured_schema[i][:column_name]}
    end
    verify_table_schema!(captured_schema, h)

    if filter && filter.any?
      h[:filter] = []

      filter.each do |k, v|
        c = h[:columns].detect { |c| c[:name] == k }
        if c
          h[:filter] << [c[:position], v]
        end
      end

      h[:filter] = :reject if h[:filter].empty? # table didn't contain the filter columns.  reject all rows.
    else
      h[:filter] = nil
    end

    h[:id_offset] = h[:columns].find_index { |i| i == 'id' }
    h[:column_names] = "(" + h[:columns].map { |c| c[:name] }.join(",") + ")"
    h
  end

  def verify_table_schema!(expected_columns, table)
    name, tcolumns = table.values_at(:name, :columns)

    if expected_columns.size != tcolumns.size
      raise SchemaChangedError.new("expected #{expected_columns.size} columns in #{table[:name]} but got #{tcolumns.size} columns")
    end

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
        t[:type] == :long_blob
      when 'varchar', 'float', 'timestamp'
        t[:type].to_s == c[:data_type]
      when 'date'
        t[:type] == :date
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
