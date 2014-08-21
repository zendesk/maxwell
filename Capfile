#!/usr/bin/env ruby
require 'zendesk/deployment'

set :application, 'zendesk_exodus_binlog_service'
set :repository, 'git@github.com:/zendesk/exodus_binlog_service.git'
set :config_files, %w(exodus_binlog_service.yml)

namespace :deploy do
  task :restart do
  end

  task :configure do
    config_files.each do |file|
      run "ln -nfs #{deploy_to}/config/#{file} #{release_path}/config/"
    end
  end

  task :stop do
  end

  task :start do
  end
end

after 'deploy:update_code', 'deploy:configure'
