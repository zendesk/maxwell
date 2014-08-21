Dir.glob(File.dirname(__FILE__) + "/../vendor/java/*.jar").each do |jar|
  require jar
end
require "java"
