class Schema
  def initialize(connection, db_name)
    @db = db_name
    @cx = connection
  end

  def fetch
    @schema = {}
    tables = @cx.query("show tables from #{@db}")
    tables.each do |row|
      table = row.values.first
      @schema[table] = []

      cols = @cx.query("select * from information_schema.columns where TABLE_SCHEMA='#{@db}' and TABLE_NAME='#{table}' ORDER BY ORDINAL_POSITION")
      cols.each do |col|
        c = col.inject({}) do |accum, array|
          k, v = *array
          k = k.downcase.to_sym
          next accum if [:table_catalog, :table_schema, :table_name, :column_key, :extra, :privileges, :column_comment].include?(k)
          accum[k] = v
          accum
        end
        @schema[table] << c
      end
    end
    @schema
  end
end
