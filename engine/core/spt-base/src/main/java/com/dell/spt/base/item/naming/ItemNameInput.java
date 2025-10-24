package com.dell.spt.base.item.naming;

import com.github.akurilov.commons.io.Input;

import java.util.List;

public interface ItemNameInput
				extends Input<String> {

	long lastId();

	enum ItemNamingType {
		RANDOM, SERIAL
	}

	@Override
	String get();

	@Override
	int get(final List<String> buffer, final int limit);

	interface Builder {

		Builder type(final ItemNamingType type);

		Builder radix(final int radix);

		Builder prefix(final String prefix);

		Builder length(final int length);

		Builder seed(final long offset);

		Builder step(final int step);

		ItemNameInput build();

		static Builder newInstance() {
			return new ItemNameInputBuilder();
		}
	}
}
