require_relative 'helper'
require 'rack/test'

require 'web'

describe "the api" do
  include Rack::Test::Methods

  def app
    Sinatra::Application
  end

  describe "/mark_binlog_top" do
    it "does some stuff" do
      get "/mark_binlog_top?db=dev1"
      expect(last_response).to be_ok
    end
  end
end
