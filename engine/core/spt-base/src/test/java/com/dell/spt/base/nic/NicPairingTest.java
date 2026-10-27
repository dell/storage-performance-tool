package com.dell.spt.base.nic;

import static com.dell.spt.base.nic.NicPlannerTest.codes;
import static com.dell.spt.base.nic.NicPlannerTest.planned;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.nic.NicPlan.PairingGroup;
import com.dell.spt.base.nic.NicPlan.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Pairing within reachability groups (item 7, D27): {@code max(E, N)} Paths, path k = (NIC k mod N, endpoint k mod E). */
class NicPairingTest {

	@TempDir
	java.nio.file.Path root;

	private FakeHost host;

	@BeforeEach
	void createHost() {
		host = new FakeHost(root);
	}

	@ParameterizedTest(name = "E={0}, N={1}")
	@CsvSource({
			"4, 4, false",
			"8, 4, false",
			"1, 4, false",
			"2, 4, false",
			"6, 4, true",
			"3, 2, true",
			"4, 3, true",
			"5, 4, true",
	})
	void ruleWithinOneGroup(final int e, final int n, final boolean uneven) {
		final var nics = nics(n, "192.0.2", 11);
		final var endpoints = endpoints(e, "192.0.2", 101);

		final var plan = plan(endpoints);

		assertEquals(nics, plan.nicSet());
		assertEquals(1, plan.groups().size());
		final var group = plan.groups().get(0);
		assertEquals(nics, group.nics());
		assertEquals(endpoints, group.endpoints());
		assertEquals(expectedPaths(nics, endpoints), group.paths());
		assertEquals(Math.max(e, n), group.paths().size());
		for (final var nic : nics) {
			assertEquals(group.paths().stream().filter(path -> path.nic().equals(nic)).count(), planned(plan, nic).paths());
		}
		assertEquals(uneven ? List.of(NicCode.UNEVEN_PAIRING) : List.of(), codes(plan.warnings()));
	}

	@ParameterizedTest(name = "slice {0} of 3, N=4")
	@CsvSource({"0", "1", "2"
	})
	void slicesArriveAsInput(final int slice) {
		final var nics = nics(4, "192.0.2", 11);
		final var all = endpoints(9, "192.0.2", 101);
		// The caller slices; the planner pairs what it is given, in the given order.
		final var given = IntStream.range(0, all.size()).filter(k -> k % 3 == slice).mapToObj(all::get).toList();

		final var plan = plan(given);

		final var group = plan.groups().get(0);
		assertEquals(given, group.endpoints());
		assertEquals(expectedPaths(nics, given), group.paths());
		assertTrue(group.paths().stream().allMatch(path -> given.contains(path.endpoint())));
		assertEquals(List.of(NicCode.UNEVEN_PAIRING), codes(plan.warnings()), "E=3 against N=4");
	}

	@Test
	void twoRailsFormTwoGroups() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24").speed(400000);
		host.physical("ens3np0", "0000:18:00.0").address("198.51.100.11/24").speed(400000);
		final var endpoints = List.of("192.0.2.101:9000", "198.51.100.101:9000", "192.0.2.102:9000", "198.51.100.102:9000");

		final var plan = plan(endpoints);

		assertEquals(
						List.of(
										new PairingGroup(
														List.of("ens2np0"),
														List.of("192.0.2.101:9000", "192.0.2.102:9000"),
														List.of(new Path("ens2np0", "192.0.2.101:9000"), new Path("ens2np0", "192.0.2.102:9000"))),
										new PairingGroup(
														List.of("ens3np0"),
														List.of("198.51.100.101:9000", "198.51.100.102:9000"),
														List.of(new Path("ens3np0", "198.51.100.101:9000"), new Path("ens3np0", "198.51.100.102:9000")))),
						plan.groups());
		assertTrue(plan.warnings().isEmpty(), plan.warnings()::toString);
	}

	@Test
	void groupsThatShareANic() {
		host.physical("ens2np0", "0000:17:00.0").address("192.0.2.11/24").speed(400000);
		host.physical("ens3np0", "0000:18:00.0").address("192.0.2.12/24").speed(400000);
		host.route("ens3np0", "198.51.100.0/24", "192.0.2.1");
		final var endpoints = List.of("192.0.2.101:9000", "198.51.100.101:9000", "198.51.100.102:9000");

		final var plan = new NicPlanner(NicPlannerTest.RESOLVER).plan(host.read(), List.of("ens2np0", "ens3np0"), endpoints, false);

		assertEquals(
						List.of(
										new PairingGroup(
														List.of("ens2np0", "ens3np0"),
														List.of("192.0.2.101:9000"),
														List.of(new Path("ens2np0", "192.0.2.101:9000"), new Path("ens3np0", "192.0.2.101:9000"))),
										new PairingGroup(
														List.of("ens3np0"),
														List.of("198.51.100.101:9000", "198.51.100.102:9000"),
														List.of(new Path("ens3np0", "198.51.100.101:9000"), new Path("ens3np0", "198.51.100.102:9000")))),
						plan.groups());
		assertEquals(3, planned(plan, "ens3np0").paths());
		assertEquals(1, planned(plan, "ens2np0").paths());
		assertEquals(List.of(NicCode.NIC_IN_SEVERAL_GROUPS), codes(plan.warnings()));
		assertEquals("ens3np0", plan.warnings().get(0).nic());
		assertTrue(plan.ok(), plan.failures()::toString);
	}

	@Test
	void unservedEndpointsFormNoGroup() {
		nics(2, "192.0.2", 11);

		final var plan = plan(List.of("192.0.2.101:9000", "203.0.113.101:9000"));

		assertEquals(List.of("192.0.2.101:9000"), plan.groups().stream().flatMap(group -> group.endpoints().stream()).toList());
		assertEquals(List.of(NicCode.ENDPOINT_UNSERVED), codes(plan.failures()));
	}

	private static List<Path> expectedPaths(final List<String> nics, final List<String> endpoints) {
		final var paths = new ArrayList<Path>();
		for (int k = 0; k < Math.max(nics.size(), endpoints.size()); k++) {
			paths.add(new Path(nics.get(k % nics.size()), endpoints.get(k % endpoints.size())));
		}
		return paths;
	}

	/** {@code count} 400G NICs on one /24, named so natural and lexical order differ. */
	private List<String> nics(final int count, final String net, final int firstHost) {
		final var names = new ArrayList<String>();
		for (int i = 0; i < count; i++) {
			final var name = "ens" + (i * 9 + 2) + "np0";
			host.physical(name, String.format("0000:%02x:00.0", 0x17 + i)).address(net + "." + (firstHost + i) + "/24").speed(400000);
			names.add(name);
		}
		return names;
	}

	private static List<String> endpoints(final int count, final String net, final int firstHost) {
		return IntStream.range(0, count).mapToObj(i -> net + "." + (firstHost + i) + ":9000").toList();
	}

	private NicPlan plan(final List<String> endpoints) {
		return new NicPlanner(NicPlannerTest.RESOLVER).plan(host.read(), List.of(NicPlanner.AUTO), endpoints, false);
	}
}
