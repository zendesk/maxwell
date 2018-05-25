package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.schema.SchemaCaptureTest;
import com.zendesk.maxwell.core.schema.ddl.DDLIntegrationTest;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Categories.class)
@Categories.IncludeCategory(Mysql57Tests.class)
@Suite.SuiteClasses({
	BootstrapIntegrationTest.class,
	SchemaCaptureTest.class,
	MaxwellIntegrationTest.class,
	DDLIntegrationTest.class,
	EmbeddedMaxwellTest.class
})
public class Mysql57TestSuite {}
