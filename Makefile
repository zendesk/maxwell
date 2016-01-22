all: compile

.make-classpath: pom.xml
	mvn dependency:copy-dependencies
	mvn dependency:build-classpath | grep -v '^\[' > .make-classpath

JAVA_SOURCES=$(shell find . -name '*.java')
ANTLR_SOURCES=$(shell find . -name '*.g4')
ANTLR=java -cp target/dependency/antlr4-4.5.jar org.antlr.v4.Tool

compile: .make-classpath
	mkdir -p target/classes target/generated-sources/annotations
	${ANTLR} -package com.zendesk.maxwell.schema.ddl \
		src/main/antlr4/com/zendesk/maxwell/schema/ddl/mysql.g4 -lib src/main/antlr4/imports
	javac -d target/classes -classpath $(shell cat .make-classpath) ${JAVA_SOURCES} \
		-s target/generated-sources/annotations -g -nowarn -target 1.7 -source 1.7 -encoding UTF-8

test:

package:


