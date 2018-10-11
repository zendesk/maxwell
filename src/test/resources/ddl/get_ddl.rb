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
  puts doc.at_css("code").text
end


base_doc = parse_url(URL)
base_doc.css(".docs-submenu .docs-sidebar-nav-link a").each do |link|
  new_link = URL.sub(%r{/[^/]*$}, '') + "/" + link.attr("href").to_s

  sub_doc = parse_url(new_link)
  extract_code(link, sub_doc)
end

