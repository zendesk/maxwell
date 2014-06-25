require_relative 'helper'
require 'rack/test'

require 'web'

describe "the api" do
  include Rack::Test::Methods

  def app
    h = {
         'binlog_dir' => $mysql_binlog_dir,
         'mysql' => {'username' => 'root', 'password' => '', 'host' => '127.0.0.1', 'port' => $mysql_master.port}
        }

    config = BinlogConfig.new
    config.config = h
    Web.set(:config, config)
    Web.new
  end

  describe "/mark_binlog_top" do
    before do
      get "/mark_binlog_top?db=shard_1"
    end

    it "returns ok" do
      expect(last_response).to be_ok
    end

    it "returns json" do

    end
  end
end
