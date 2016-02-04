all: compile

MAXWELL_VERSION=$(shell build/current_rev)

JAVAC=javac
JAVAC_FLAGS += -d target/classes
JAVAC_FLAGS += -sourcepath src/main/java:src/test/java:target/generated-sources/src/main/antlr4
JAVAC_FLAGS += -classpath `cat .make-classpath`
JAVAC_FLAGS += -g -target 1.7 -source 1.7 -encoding UTF-8 -Xlint:-options -Xlint:unchecked

# files that just get copied to the root of the maxwell distro
DISTFILES=README.md docs/docs/quickstart.md docs/docs/config.md LICENSE src/main/resources/log4j2.xml

CLASSPATH=target/.make-classpath

$(CLASSPATH): pom.xml
	mkdir -p target
	mvn dependency:copy-dependencies
	mvn dependency:build-classpath | grep -v '^\[' > $(CLASSPATH)

ANTLR=java -cp target/dependency/antlr4-4.5.jar org.antlr.v4.Tool
ANTLR_SRC=src/main/antlr4/com/zendesk/maxwell/schema/ddl/mysql.g4
ANTLR_IMPORTS=src/main/antlr4/imports
ANTLR_DIR=target/generated-sources/src/main/antlr4/com/zendesk/maxwell/schema/ddl
ANTLR_OUTPUT=$(ANTLR_DIR)/mysqlBaseListener.java $(ANTLR_DIR)/mysqlLexer.java $(ANTLR_DIR)/mysqlListener.java $(ANTLR_DIR)/mysqlParser.java

$(ANTLR_OUTPUT): $(ANTLR_SRC) $(ANTLR_IMPORTS)/*.g4
	${ANTLR} -package com.zendesk.maxwell.schema.ddl -lib $(ANTLR_IMPORTS) -o target/generated-sources $(ANTLR_SRC)

compile-antlr: $(ANTLR_OUTPUT)

JAVA_SOURCE = $(shell find src/main/java -name '*.java')

target/.java: $(ANTLR_OUTPUT) $(JAVA_SOURCE)
	@mkdir -p target/classes
	$(JAVAC) $(JAVAC_FLAGS) $?
	cp -a src/main/resources/* target/classes
	@touch target/.java

compile-java: target/.java
compile: $(CLASSPATH) compile-antlr compile-java

JAVA_TEST_SOURCE=$(shell find src/test/java -name '*.java')
target/.java-test: $(JAVA_TEST_SOURCE)
	@mkdir -p target/test-classes
	cp -a src/test/resources/* target/test-classes
	javac -d target/test-classes -sourcepath src/main/java:src/test/java:target/generated-sources/src/main/antlr4 -classpath target/classes:`cat $(CLASSPATH)` \
		-g -target 1.7 -source 1.7 -encoding UTF-8 $?
	@touch target/.java-test

compile-test: $(CLASSPATH) compile target/.java-test

clean:
	rm -f  target/.java target/.java-test
	rm -rf target/classes
	rm -rf target/generated-sources
	rm -f  target/*.jar

depclean: clean
	rm -f $(CLASSPATH)

TEST_CLASSES=$(shell build/get-test-classes)

test: $(CLASSPATH) compile-test
	java -classpath `cat $(CLASSPATH)`:target/test-classes:target/classes org.junit.runner.JUnitCore $(TEST_CLASSES)

test.%: $(CLASSPATH) compile-test
	java -classpath `cat $(CLASSPATH)`:target/test-classes:target/classes org.junit.runner.JUnitCore $(filter %$(subst test.,,$@),$(TEST_CLASSES))


PKGNAME=maxwell-${MAXWELL_VERSION}
MAVEN_DIR=target/classes/META-INF/maven/com.zendesk/maxwell
MAXWELL_JARFILE=target/$(PKGNAME).jar

package-jar: all
	rm -f ${MAXWELL_JAR}
	mkdir -p ${MAVEN_DIR}
	sed -e "s/VERSION/${MAXWELL_VERSION}/" build/pom.properties > ${MAVEN_DIR}/pom.properties
	cp pom.xml ${MAVEN_DIR}
	jar cvf ${MAXWELL_JARFILE} -C target/classes .

TARDIR=target/$(PKGNAME)
TARFILE=target/$(PKGNAME).tar.gz

package-tar: $(CLASSPATH)
	rm -Rf target/dependency
	mvn dependency:copy-dependencies -DincludeScope=runtime
	rm -Rf $(TARDIR) $(TARFILE)
	mkdir $(TARDIR)
	cp $(DISTFILES) $(TARDIR)
	cp -a bin $(TARDIR)
	mkdir $(TARDIR)/lib
	cp -a $(MAXWELL_JARFILE) target/dependency/* $(TARDIR)/lib
	tar czvf $(TARFILE) -C target $(PKGNAME)

package: depclean package-jar package-tar


