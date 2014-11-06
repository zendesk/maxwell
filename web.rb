require 'bundler/setup'
$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'sinatra'
require 'json'
require 'lib/setup_java'
require 'lib/schema'
require 'lib/binlog_dir'
require 'lib/config'

set :run, false

class Web < Sinatra::Base
  set :config, BinlogConfig.new
  set :server, 'puma'
  set :port, settings.config.api_port
  set :bind, '0.0.0.0'
  set :run, false
  set :show_exceptions, false

  def require_params(*required)
    missing = required.select { |p| !params[p] }
    if missing.any?
      status 422
      body({err: "Please provide the following parameter(s): #{missing.join(',')}"}.to_json)
      false
    else
      true
    end
  end

  before do
    content_type :json
  end

  error do
    e = env['sinatra.error']
    status 500
    body({err: e.message, type: e.class.name, backtrace: e.backtrace}.to_json)
  end

  get "/ping" do
    body({status: 'ok'}.to_json)
    status 200
  end

  # capture the schema at a position.
  #
  # return both the schema and the current position of the binlog for future requests
  # required parameter: db
  get "/mark_binlog_top" do
    return unless require_params(:db)

    schema = Schema.new(settings.config.mysql_connection, params[:db])
    schema.fetch
    schema.save

    ret = {
      file:            schema.binlog_info[:file],
      pos:             schema.binlog_info[:pos],
      schema_token:    schema.filename,
      schema:          schema.fetch
    }

    body ret.to_json
    status 200
  end

  # get the current schema.  no need to save it away for later.
  get "/schema" do
    return unless require_params(:db)

    schema = Schema.new(settings.config.mysql_connection, params[:db])

    body({schema: schema.fetch}.to_json)
    status 200
  end

  # begin playing the binlog from a specified position.
  # The position *must* be one returned from mark_binlog_top
  #
  # required parameters: account_id, db, schema_token, start_file, start_pos
  # optional paramerers: end_file, end_pos
  #
  get "/binlog_events" do
    return unless require_params(:account_id, :db, :schema_token, :start_file, :start_pos)
    schema = Schema.load(params[:schema_token])

    if schema.nil?
      status 404
      body({err: "No stored schema found for token #{params[:schema_token]} -- call mark_binlog_top",
            type: "no_stored_schema"}.to_json)
      return
    end

    begin
      sql = []
      start_info = { file: params[:start_file], pos: params[:start_pos].to_i }

      if params[:end_file] && params[:end_pos]
        end_info = {file: params[:end_file], pos: params[:end_pos].to_i}
      end

      d = BinlogDir.new(settings.config.binlog_dir, schema)

      filter = { 'account_id' => params[:account_id].to_i }
      options = {}
      options[:max_events] = params[:max_events].to_i if params[:max_events]
      options[:exclude_tables] = params[:exclude_tables] ? params[:exclude_tables].split(',') : nil

      info = d.read_binlog(start_info, end_info, options) do |event|
        s = event.to_sql(filter)
        sql << s if s
      end

      next_pos = {file: info[:file], pos: info[:pos]}
      processed = info[:processed]

      status 200
      body({next_pos: next_pos, processed: info[:processed], sql: sql}.to_json)

    rescue SchemaChangedError => e
      status 500
      body({err: e.message, type: 'schema_changed'}.to_json)
    end
  end
end

if __FILE__ == $0
  Web.run!
end
