KAFKA_VERSION ?= 0.11.0.1
KAFKA_PROFILE = kafka-${KAFKA_VERSION}

all: compile

test:
	mvn test -P ${KAFKA_PROFILE}

compile:
	mvn compile -P ${KAFKA_PROFILE}

clean:
	mvn clean

depclean: clean
	rm -f $(CLASSPATH)

package: depclean kafka-0.8.2.2 kafka-0.9.0.1 kafka-0.10.0.1 kafka-0.10.2.1 kafka-0.11.0.1
	@# TODO: this is inefficient, we really just want to copy the jars...
	mvn package -DskipTests=true

kafka-%:
	mvn compile -P kafka-$(*)
