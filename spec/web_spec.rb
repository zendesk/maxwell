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

  describe "/ping" do
    it "returns ok" do
      get "/ping"
      expect(last_response).to be_ok
      expect(JSON.parse(last_response.body)).to eq("status" => "ok")
    end
  end

  describe "/mark_binlog_top" do
    before do
      get "/mark_binlog_top?db=shard_1"
      @json = JSON.parse(last_response.body)
    end

    it "returns ok" do
      expect(last_response).to be_ok
    end

    it "returns the current schema" do
      expect(@json).to_not be_nil
      expect(@json['schema'].keys).to eq(['ints', 'mediumints', 'minimal', 'sharded'])
    end

    it "returns the current position" do
      expect(@json['file']).to be_a_kind_of(String)
      expect(@json['pos']).to be_a_kind_of(Fixnum)
    end

    it "returns a token to be stored and passed in future reqs" do
      expect(@json['schema_token']).to be_a_kind_of(String)
    end
  end

  describe "/schema" do
    before do
      get "/schema?db=shard_1"
      @json = JSON.parse(last_response.body)
    end

    it "returns the current schema" do
      expect(@json).to_not be_nil
      expect(@json['schema'].keys).to eq(['ints', 'mediumints', 'minimal', 'sharded'])
    end
  end

  describe "/binlog_events" do
    it "422s without 4 required parameters" do
      get '/binlog_events'
      expect(last_response.status).to eq(422)
      body = JSON.parse(last_response.body)
      expect(body).to eq('err' => 'Please provide the following parameter(s): account_id,db,schema_token,start_file,start_pos')
    end

    it "404s when you fail to call /mark_binlog_top" do
      get '/binlog_events?account_id=1&db=shard_1&start_file=foo&start_pos=43&schema_token=abc'
      expect(last_response.status).to eq(404)
    end

    describe "when called properly" do
      before do
        reset_master
        get '/mark_binlog_top?account_id=1&db=shard_1'
        json = JSON.parse(last_response.body)
        file, pos, token = json.values_at('file', 'pos', 'schema_token')
        generate_binlog_events
        get('/binlog_events', account_id: 1, db: 'shard_1', start_file: file, start_pos: pos, schema_token: token)
      end

      it "returns 200" do
        expect(last_response).to be_ok
      end

      it "returns events" do
        json = JSON.parse(last_response.body)
        expect(json['sql']).to_not be_empty
      end

      it "returns the next position to start from" do
        json = JSON.parse(last_response.body)
        expect(json['next_pos']['file']).to eq('master.000002')
        expect(json['next_pos']['pos']).to be_a_kind_of(Fixnum)
      end

      it "returns the number of events processed" do
        json = JSON.parse(last_response.body)
        expect(json['processed']).to be_a_kind_of(Fixnum)
      end
    end

    describe "when called with max_events" do
      before do
        reset_master
        get '/mark_binlog_top?account_id=1&db=shard_1'
        json = JSON.parse(last_response.body)
        file, pos, token = json.values_at('file', 'pos', 'schema_token')
        generate_binlog_events
        get('/binlog_events', account_id: 1, db: 'shard_1', start_file: file, start_pos: pos, schema_token: token, max_events: 2)
      end

      it "will only process that many events" do
        json = JSON.parse(last_response.body)
        expect(json['processed']).to eq(2)
      end
    end
  end
end
