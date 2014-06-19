class BinlogEvent
  java_import "org.apache.commons.lang.StringEscapeUtils"
  attr_reader :type, :attrs

  def initialize(type, table, attrs, column_names)
    @type = type
    @table = table
    @attrs = attrs
    @column_names = column_names
  end

  def to_sql
    if @type == :delete
      delete_to_sql
    else
      replace_to_sql
    end
  end

  def delete_to_sql
    "DELETE FROM `#{@table}` WHERE id = #{@attrs['id']}"
  end

  def escape(s)
    case s
    when String
      "'" + StringEscapeUtils.escapeSql(s) + "'"
    when nil
      'NULL'
    else
      s.to_s
    end
  end

  def replace_to_sql
    values = @column_names.map do |c|
      escape(@attrs[c.to_s])
    end
    columns = @column_names.join(", ")
    "REPLACE INTO `#{@table}` (#{columns}) VALUES (" + values.join(", ") + ")"
  end
end
