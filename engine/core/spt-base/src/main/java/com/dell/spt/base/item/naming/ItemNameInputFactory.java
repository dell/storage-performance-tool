package com.dell.spt.base.item.naming;

import static com.dell.spt.base.config.el.Language.withLanguage;

import com.dell.spt.base.logging.LogUtil;
import com.github.akurilov.commons.io.el.ExpressionInput;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.confuse.Config;
import java.util.Locale;
import org.apache.logging.log4j.Level;

/** Builds item-name inputs from the shared item naming configuration. */
public final class ItemNameInputFactory {

	private ItemNameInputFactory() {}

	public static ItemNameInput fromConfig(final Config namingConfig) {
		final int length = namingConfig.intVal("length");
		final var seedRaw = namingConfig.val("seed");
		long seed = 0;
		try {
			seed = TypeUtil.typeConvert(seedRaw, long.class);
		} catch (final ClassCastException | NumberFormatException e) {
			if (seedRaw instanceof String) {
				try (
								final var in = withLanguage(ExpressionInput.builder())
												.expression((String) seedRaw)
												.<ExpressionInput<Long>> build()) {
					seed = in.get();
				} catch (final Exception expressionFailure) {
					LogUtil.exception(
									Level.WARN,
									expressionFailure,
									"Item naming seed expression (\"{}\") failure",
									seedRaw);
				}
			} else {
				throw new IllegalStateException(
								"Item naming seed (" + seedRaw + ") should be an integer or an expression", e);
			}
		}
		final var shardCountRaw = namingConfig.val("shards");
		final int shardCount = shardCountRaw == null
						? 0
						: TypeUtil.typeConvert(shardCountRaw, int.class);
		return ItemNameInput.Builder.newInstance()
						.length(length)
						.seed(seed)
						.prefix(namingConfig.stringVal("prefix"))
						.shardCount(shardCount)
						.radix(namingConfig.intVal("radix"))
						.step(namingConfig.intVal("step"))
						.type(ItemNameInput.ItemNamingType.valueOf(
										namingConfig.stringVal("type").toUpperCase(Locale.ROOT)))
						.build();
	}
}
