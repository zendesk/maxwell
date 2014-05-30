require 'bundler/setup'
$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'sinatra'
require 'json'
require 'lib/schema'
require 'lib/binlog_dir'
require 'lib/config'

class Web < Sinatra::Base
  set :config, BinlogConfig.new

  def require_params(*required)
    missing = required.select { |p| !params[p] }
    if missing.any?
      status 422
      body(err: "Please provide the following parameter(s): #{mising.join(',')}")
      false
    else
      true
    end
  end

  before do
    content_type :json
  end

  # capture the schema at a position.
  #
  # return both the schema and the current position of the binlog for future requests
  # required parameter: database
  get "/mark_binlog_top" do
    return unless require_params(:db)

    schema = Schema.new(settings.config.mysql_connection, params[:db])
    s = schema.fetch
    schema.save
    status 200
    body schema.binlog_info.merge(schema: s).to_json
  end

  # begin playing the binlog from a specified position.
  # The position *must* be one returned from mark_binlog_top
  #
  # required parameters: account_id, db, start_file, start_pos
  # optional paramerers: end_file, end_pos
  #
  get "/binlog_events" do
    return unless require_params(:account_id, :db, :start_file, :start_pos)
    schema = Schema.load(params[:db], params[:start_file], params[:start_pos].to_i)

    if schema.nil?
      status 404
      body({err: "No stored schema found for #{params[:start_file]}:#{params[:start_pos]} -- call mark_binlog_top"})
      return
    end

    begin
      b = BinlogDir.new(settings.config.binlog_dir, schema)
      events = []

      filter = { account_id: params[:account_id].to_i }
      start_info = { file: params[:start_file], pos: params[:start_pos] }
      if params[:end_file] && params[:end_pos]
        end_info = {file: params[:end_file], pos: params[:end_pos]}
      end

      next_pos = b.read_binlog(filter, start_info, end_info, 1000) do |event|

      end
    rescue SchemaChangedError => e

    end
  end
end

