repositories.remote << 'http://www.ibiblio.org/maven2'

define 'maxwell' do
  project.version = '0.14.6'
  compile.with transitive('mysql:mysql-connector-java:jar:5.1.6')
  compile.with transitive('commons-lang:commons-lang:jar:2.6')
  compile.with transitive('commons-codec:commons-codec:jar:1.5')
  package :tgz
end
