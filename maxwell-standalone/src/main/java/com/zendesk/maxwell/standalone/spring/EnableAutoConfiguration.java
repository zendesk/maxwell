package com.zendesk.maxwell.standalone.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(AutoConfigurationImportSelector.class)
public @interface EnableAutoConfiguration { }
