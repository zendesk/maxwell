all: compile

MAXWELL_VERSION=$(shell build/current_rev)

JAVAC=javac
JAVAC_FLAGS += -d target/classes
JAVAC_FLAGS += -sourcepath src/main/java:src/test/java:target/generated-sources/src/main/antlr4
JAVAC_FLAGS += -g -target 1.7 -source 1.7 -encoding UTF-8 -Xlint:-options -Xlint:unchecked

# files that just get copied to the root of the maxwell distro
DISTFILES=README.md docs/docs/quickstart.md docs/docs/config.md LICENSE src/main/resources/log4j2.xml

ANTLR_DEPS=$(shell build/maven_fetcher -f org.antlr/antlr4/4.5 -o target/dependency-antlr)
ANTLR=java -cp $(ANTLR_DEPS) org.antlr.v4.Tool
ANTLR_SRC=src/main/antlr4/com/zendesk/maxwell/schema/ddl/mysql.g4
ANTLR_IMPORTS=src/main/antlr4/imports
ANTLR_DIR=target/generated-sources/src/main/antlr4/com/zendesk/maxwell/schema/ddl
ANTLR_OUTPUT=$(ANTLR_DIR)/mysqlBaseListener.java $(ANTLR_DIR)/mysqlLexer.java $(ANTLR_DIR)/mysqlListener.java $(ANTLR_DIR)/mysqlParser.java

$(ANTLR_OUTPUT): $(ANTLR_SRC) $(ANTLR_IMPORTS)/*.g4
	${ANTLR} -package com.zendesk.maxwell.schema.ddl -lib $(ANTLR_IMPORTS) -o target/generated-sources $(ANTLR_SRC)

compile-antlr: $(ANTLR_OUTPUT)

JAVA_SOURCE = $(shell find src/main/java -name '*.java')
JAVA_DEPENDS = $(shell  build/maven_fetcher -p -o target/dependency)

target/.java: $(ANTLR_OUTPUT) $(JAVA_SOURCE)
	@mkdir -p target/classes
	$(JAVAC) -classpath $(JAVA_DEPENDS) $(JAVAC_FLAGS) $?
	cp -a src/main/resources/* target/classes
	@touch target/.java

compile-java: target/.java
compile: compile-antlr compile-java



JAVA_TEST_DEPENDS = $(shell build/maven_fetcher -p -o target/dependency-test -s test)
JAVA_TEST_SOURCE=$(shell find src/test/java -name '*.java')
target/.java-test: $(JAVA_TEST_SOURCE)
	@mkdir -p target/test-classes
	cp -a src/test/resources/* target/test-classes
	javac -d target/test-classes -sourcepath src/main/java:src/test/java:target/generated-sources -classpath target/classes:$(JAVA_TEST_DEPENDS) \
		-g -target 1.7 -source 1.7 -encoding UTF-8 $?
	@touch target/.java-test

compile-test: compile target/.java-test

TEST_CLASSES=$(shell build/get-test-classes)

test: compile-test
	java -Xmx128m -classpath $(JAVA_TEST_DEPENDS):target/test-classes:target/classes org.junit.runner.JUnitCore $(TEST_CLASSES)

test.%:  compile-test
	java -classpath $(JAVA_TEST_DEPENDS):target/test-classes:target/classes org.junit.runner.JUnitCore $(filter %$(subst test.,,$@),$(TEST_CLASSES))


clean:
	rm -f  target/.java target/.java-test
	rm -rf target/classes
	rm -rf target/generated-sources
	rm -f  target/*.jar

depclean: clean
	rm -f $(CLASSPATH)


PKGNAME=maxwell-${MAXWELL_VERSION}
MAVEN_DIR=target/classes/META-INF/maven/com.zendesk/maxwell
MAXWELL_JARFILE=target/$(PKGNAME).jar

package-jar: all
	rm -f ${MAXWELL_JARFILE}
	mkdir -p ${MAVEN_DIR}
	sed -e "s/VERSION/${MAXWELL_VERSION}/" build/pom.properties > ${MAVEN_DIR}/pom.properties
	cp pom.xml ${MAVEN_DIR}
	jar cvf ${MAXWELL_JARFILE} -C target/classes .

TARDIR=target/$(PKGNAME)
TARFILE=target/$(PKGNAME).tar.gz

package-tar:
	rm -Rf target/dependency-build
	build/maven_fetcher -p -o target/dependency-build >/dev/null
	rm -Rf $(TARDIR) $(TARFILE)
	mkdir $(TARDIR)
	cp $(DISTFILES) $(TARDIR)
	cp -a bin $(TARDIR)
	mkdir $(TARDIR)/lib
	cp -a $(MAXWELL_JARFILE) target/dependency-build/* $(TARDIR)/lib
	tar czvf $(TARFILE) -C target $(PKGNAME)

package: depclean package-jar package-tar


