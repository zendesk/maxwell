require 'bundler/setup'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)

ROOT=File.dirname(__FILE__)
task :jars do
  system("cd #{ROOT}/java && mvn package")
  system("find #{ROOT}/java/target -name '*.jar' | xargs -I{} cp {} vendor/java") 
end
task :default => :spec
