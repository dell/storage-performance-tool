package com.dell.spt.base.item;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import com.dell.spt.base.config.TestConfigBuilder;
import com.github.akurilov.confuse.Config;

public class ItemTypeFactoryTest {
	Config testConfig = TestConfigBuilder.config();

	/*
	 	Description: Reading the item-type from a configuration object should typecast to an ItemType, which
	    can be used to create an ItemFactory
	    Steps:
			1. Read item-type from a configuration object
	        2. Insert the supposed itemType into the factory pattern
	        3. Ensure we get the proper factory implementation based off of the configuration value
	 */
	@Test
	public void dataItemFactoryTest() {
		testConfig.val("item-type", "data");
		final ItemType itemType = ItemType.valueOf(testConfig.stringVal("item-type").toUpperCase());
		final ItemFactory<Item> itemFactory = ItemType.getItemFactory(itemType);
		assertInstanceOf(DataItemFactoryImpl.class, itemFactory);
	}

	/*
	 	Description: Reading the item-type from a configuration object should typecast to an ItemType, which
	    can be used to create an ItemFactory
	    Steps:
			1. Read item-type from a configuration object
	        2. Insert the supposed itemType into the factory pattern
	        3. Ensure we get the proper factory implementation based off of the configuration value
	 */
	@Test
	public void pathItemFactoryTest() {
		testConfig.val("item-type", "path");
		final ItemType itemType = ItemType.valueOf(testConfig.stringVal("item-type").toUpperCase());
		final ItemFactory<Item> itemFactory = ItemType.getItemFactory(itemType);
		assertInstanceOf(PathItemFactoryImpl.class, itemFactory);
	}

	/*
	 	Description: Reading the item-type from a configuration object should typecast to an ItemType, which
	    can be used to create an ItemFactory
	    Steps:
			1. Read item-type from a configuration object
	        2. Insert the supposed itemType into the factory pattern
	        3. Ensure we get the proper factory implementation based off of the configuration value
	 */
	@Test
	public void tokenItemFactoryTest() {
		testConfig.val("item-type", "token");
		final ItemType itemType = ItemType.valueOf(testConfig.stringVal("item-type").toUpperCase());
		final ItemFactory<Item> itemFactory = ItemType.getItemFactory(itemType);
		assertInstanceOf(TokenItemFactoryImpl.class, itemFactory);
	}
}
