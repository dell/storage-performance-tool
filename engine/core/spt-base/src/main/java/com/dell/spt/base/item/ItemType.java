package com.dell.spt.base.item;

/** Created by kurila on 28.03.16. */
public enum ItemType {
	DATA, PATH, TOKEN;

	@SuppressWarnings("unchecked")
	public static ItemFactory<? extends Item> getItemFactory(final ItemType itemType) {
		if (ItemType.DATA.equals(itemType)) {
			return new DataItemFactoryImpl<>();
		} else if (ItemType.PATH.equals(itemType)) {
			return new PathItemFactoryImpl<>();
		} else if (ItemType.TOKEN.equals(itemType)) {
			return new TokenItemFactoryImpl<>();
		} else {
			throw new AssertionError("Item type \"" + itemType + "\" is not supported");
		}
	}
}
