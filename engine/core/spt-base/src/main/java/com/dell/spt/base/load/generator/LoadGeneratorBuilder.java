package com.dell.spt.base.load.generator;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.github.akurilov.commons.concurrent.throttle.IndexThrottle;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.IOException;

/** Created by andrey on 12.11.16. */
public interface LoadGeneratorBuilder<I extends Item, O extends Operation<I>, T extends LoadGenerator<I, O>> {

	LoadGeneratorBuilder<I, O, T> itemConfig(final Config itemConfig);

	LoadGeneratorBuilder<I, O, T> loadConfig(final Config loadConfig);

	LoadGeneratorBuilder<I, O, T> itemType(final ItemType itemType);

	LoadGeneratorBuilder<I, O, T> itemFactory(final ItemFactory<I> itemFactory);

	LoadGeneratorBuilder<I, O, T> authConfig(final Config authConfig);

	LoadGeneratorBuilder<I, O, T> loadOperationsOutput(Output<O> storageDriver);

	LoadGeneratorBuilder<I, O, T> itemInput(final Input<I> itemInput);

	LoadGeneratorBuilder<I, O, T> originIndex(final int originIndex);

	LoadGeneratorBuilder<I, O, T> addThrottle(final Throttle throttle);

	LoadGeneratorBuilder<I, O, T> addThrottle(final IndexThrottle throttle);

	T build() throws IllegalConfigurationException, IOException;

	default ListShardMetricsRecorder listShardMetricsRecorder() {
		return ListShardMetricsRecorder.NO_OP;
	}
}
