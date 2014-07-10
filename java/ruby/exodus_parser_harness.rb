#!/usr/bin/env ruby

include Java

Dir.glob(File.dirname(__FILE__) + "/../target/**/*.jar").each do |f|
  require f
end


#require_relative '../target/exodus-open-replicator-0.0.1.jar'
#require_relative '../target/exodus-open-replicator-0.0.1.jar'
java_import 'com.zendesk.exodus.ExodusParser'


p = ExodusParser.new('/opt/local/var/db/mysql5', 'master.000004')
puts "entering getEvent"
count = 0
while (e = p.getEvent())
  count += 1
  puts count if count % 1000 == 0
  #puts e
end

