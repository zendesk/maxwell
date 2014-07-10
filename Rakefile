require 'bundler/setup'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)

ROOT=File.dirname(__FILE__)
task :jars do
  system("cd #{ROOT}/../open-replicator && mvn package")
  system("cd #{ROOT}/java && mvn package")
  system("find #{ROOT}/java/target -name '*.jar' | xargs -I{} cp {} vendor/java") 
  system("cp #{ROOT}/../open-replicator/target/open-replicator-1.0.7.jar vendor/java")
end
task :default => :spec
