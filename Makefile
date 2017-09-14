all: compile

test:
	mvn test -Dkafka_version=${KAFKA_VERSION}

compile:
	mvn compile -Dkafka_version=${KAFKA_VERSION}

clean:
	mvn clean

depclean: clean
	rm -f $(CLASSPATH)

package: depclean
	mvn package -Dkafka_version=${KAFKA_VERSION}
