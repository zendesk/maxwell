class BinlogEvent
  attr_reader :type, :attrs

  def initialize(type, table, attrs, columns)
    @type = type
    @table = table
    @attrs = attrs
    @columns = columns
  end

  def to_sql
    if @type == 'delete'
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
      "'" + Mysql2::Client.escape(s) + "'"
    else
      s.to_s
    end
  end

  def replace_to_sql
    values = @columns.map do |c|
      escape(@attrs[c.to_s])
    end
    columns = @columns.join(", ")
    "REPLACE INTO `#{@table}` (#{columns}) VALUES (" + values.join(", ") + ")"
  end
end
