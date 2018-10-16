#!/usr/bin/env ruby

require 'bundler/setup'
require 'oga'
require 'net/http'

VERSION=ARGV[0]
URL=ARGV[1]

def parse_url(url)
  html = Net::HTTP.get(URI(url))
  document = Oga.parse_html(html)
end

def extract_code(link_name, doc)
  code = doc.at_css("code")
  return unless code
  filename = VERSION + "/" + link_name.sub(/\..*$?/, '')
  File.open(filename, "w+") do |file|
    file.write(code.text)
  end
end


base_doc = parse_url(URL)
base_doc.css(".docs-submenu .docs-sidebar-nav-link a").each do |link|
  new_link = URL.sub(%r{/[^/]*$}, '') + "/" + link.attr("href").to_s

  sub_doc = parse_url(new_link)
  extract_code(link.attr("href").to_s, sub_doc)
end

