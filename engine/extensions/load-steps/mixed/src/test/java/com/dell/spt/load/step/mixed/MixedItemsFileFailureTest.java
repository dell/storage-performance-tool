package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.logging.Loggers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A mix that lost items from its files isn't the mix that was asked for. */
class MixedItemsFileFailureTest {

	private static final String GOOD = "/b/a,0,10,0/0\n";

	@TempDir
	Path dir;

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	private String file(final String content) throws Exception {
		return Files.writeString(dir.resolve("items.csv"), content).toString();
	}

	private static PoolItemInput pool() {
		return new PoolItemInput(List.of(new DataItemFactoryImpl<>().getItem(GOOD.trim())));
	}

	private static String seedFailure(final String path) {
		final var failure = assertThrows(
						RunTerminalException.class,
						() -> MixedLoadStepLocal.loadSeedItems(path, new ArrayList<>(), new DataItemFactoryImpl<>()));
		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		return failure.getMessage();
	}

	private static String deleteFailure(final String path) {
		final var failure = assertThrows(
						RunTerminalException.class,
						() -> MixedLoadStepLocal.loadDeleteItems(path, pool(), new DataItemFactoryImpl<>()));
		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		return failure.getMessage();
	}

	@Test
	void seedItemsLoadInFull() throws Exception {
		final List<Object> items = new ArrayList<>();
		MixedLoadStepLocal.loadSeedItems(file(GOOD + GOOD), items, new DataItemFactoryImpl<>());
		assertEquals(2, items.size());
	}

	@Test
	void aBadSeedLineIsNamed() throws Exception {
		assertEquals("items file line 2 is blank", seedFailure(file(GOOD + "\n" + GOOD)));
		assertEquals("items file line 2 is not a valid item", seedFailure(file(GOOD + "/b/x,zz,400,0/0\n" + GOOD)));
	}

	@Test
	void aMissingSeedFileFails() {
		assertEquals("MixedLoad: seed items file was not found", seedFailure(dir.resolve("none.csv").toString()));
	}

	@Test
	void aSeedFileThatCantBeOpenedFails() throws Exception {
		final String notCsv = Files.writeString(dir.resolve("items.bin"), "").toString();
		assertEquals("MixedLoad: seed items file could not be opened", seedFailure(notCsv));
	}

	@Test
	void aBadDeleteLineIsNamed() throws Exception {
		assertEquals("items file line 2 is blank", deleteFailure(file(GOOD + "\n" + GOOD)));
	}

	@Test
	void anEmptyOrMissingDeleteFileFails() throws Exception {
		assertEquals("MixedLoad: delete items file has no items", deleteFailure(file("")));
		assertEquals("MixedLoad: delete items file was not found", deleteFailure(dir.resolve("none.csv").toString()));
	}
}
