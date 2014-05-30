require 'bundler/setup'
$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'sinatra'
require 'json'
require 'lib/schema'
require 'lib/binlog_dir'
require 'lib/config'

class Web < Sinatra::Base
  set :config, BinlogConfig.new

  def require_params(*params)
    missing = params.select { |p| !params[p] }
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
  # required parameters: account_id, database, start_file, start_pos
  # optional paramerers: end_file, end_pos
  #
  get "/binlog_events" do


  end



