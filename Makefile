all: compile

.make-classpath: pom.xml
	mvn dependency:copy-dependencies
	mvn dependency:build-classpath | grep -v '^\[' > .make-classpath

ANTLR=java -cp target/dependency/antlr4-4.5.jar org.antlr.v4.Tool
CHANGED_ANTLR_SOURCES=$(shell build/get-changed-files .make-last-compile '*.g4')

antlr:
ifneq ($(strip $(CHANGED_ANTLR_SOURCES)),)
	${ANTLR} -package com.zendesk.maxwell.schema.ddl src/main/antlr4/com/zendesk/maxwell/schema/ddl/mysql.g4 -lib src/main/antlr4/imports
endif

CHANGED_JAVA_SOURCES=$(shell build/get-changed-files .make-last-compile '*.java')

compile: .make-classpath antlr
ifneq ($(strip $(CHANGED_JAVA_SOURCES)),)
	mkdir -p target/classes
	javac -d target/classes -sourcepath src/main/java:src/main/antlr4 -classpath `cat .make-classpath` \
		-g -nowarn -target 1.7 -source 1.7 -encoding UTF-8 ${CHANGED_JAVA_SOURCES}
	touch .make-last-compile
endif

test:

package:


