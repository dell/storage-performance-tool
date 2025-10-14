package com.dell.spt.base.item.op.path;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PathOperationsBuilderImplTest {

	@Test
	void buildSingleOperationUsesProvidedCredentialAndType() throws Exception {
		final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(7)
						.opType(OpType.UPDATE)
						.outputPathInput(new ConstantValueInputImpl<>("/dst"));

		final var cred = Credential.getInstance("u", "s");
		builder.credentialInput(new ConstantValueInputImpl<>(cred));

		final var item = new PathItemImpl("/src/name");
		final var op = builder.buildOp(item);

		assertEquals(7, op.originIndex());
		assertEquals(OpType.UPDATE, op.type());
		assertSame(item, op.item());
		assertEquals(cred, op.credential());
	}

	@Test
	void buildListOperationReturnsListImplementation() throws Exception {
		final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(3)
						.opType(OpType.LIST);

		final var item = new PathItemImpl("/bucket/prefix");
		final var op = builder.buildOp(item);

		assertEquals(OpType.LIST, op.type());
		assertTrue(op instanceof com.dell.spt.base.item.op.list.ListOperation);
	}

	@Test
	void listOperationRebasesItemAndAppliesOptions() throws Exception {
		final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(5);
		builder.opType(OpType.LIST);
		builder.inputPath("/bucket");
		builder.listOptions(ListOptions.builder().delimiter("/").maxKeys(123).build());

		final var item = new PathItemImpl("/bucket/prefix/object");
		final var op = builder.buildOp(item);

		assertTrue(op instanceof ListOperation);
		final var listOp = (ListOperation<PathItemImpl>) op;
		assertEquals("/bucket", listOp.srcPath());
		assertEquals("prefix/object", listOp.item().name());
		assertEquals("/", listOp.options().delimiter());
		assertEquals(123, listOp.options().maxKeys());
	}

	@Test
	void listOperationReceivesShardMetadata() throws Exception {
		final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(1);
		builder.opType(OpType.LIST);
		builder.inputPath("/bucket");
		final var shard = new com.dell.spt.base.item.op.list.shard.ListShard("logs/a", null, null, null);
		builder.listShardingContext(new com.dell.spt.base.item.op.list.shard.ListShardingContext(List.of(shard)));
		final var item = new PathItemImpl("/bucket/logs/a");
		final var op = builder.buildOp(item);
		assertTrue(op instanceof com.dell.spt.base.item.op.list.ListOperationImpl);
		final var listOp = (com.dell.spt.base.item.op.list.ListOperationImpl<PathItemImpl>) op;
		assertEquals("logs/a", listOp.item().name());
		assertEquals(shard, listOp.shard());
	}

	// @Test
	// void buildBatchOperationsRespectsPathMappedCredentials() throws Exception {
	//     final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(1)
	//             .opType(OpType.CREATE);

	//     final var outPaths = List.of("/a", "/b", "/a");
	//     final var items = new ArrayList<PathItemImpl>();
	//     for (int i = 0; i < outPaths.size(); i++) {
	//         items.add(new PathItemImpl("name-" + i));
	//     }

	//     // Simulate cycling output paths via ConstantValueInputImpl per call
	//     final var iterator = outPaths.iterator();
	//     builder.outputPathInput(new ConstantValueInputImpl<>(null) {
	//         @Override public String get() { return iterator.next(); }
	//     });

	//     final var cA = Credential.getInstance("A", "a");
	//     final var cB = Credential.getInstance("B", "b");
	//     final var map = new HashMap<String, Credential>();
	//     map.put("/a", cA);
	//     map.put("/b", cB);
	//     builder.credentialsByPath(map);

	//     final var buff = new ArrayList<PathOperation<PathItemImpl>>();
	//     builder.buildOps(items, buff);
	//     assertEquals(items.size(), buff.size());
	//     assertEquals(cA, buff.get(0).credential());
	//     assertEquals(cB, buff.get(1).credential());
	//     assertEquals(cA, buff.get(2).credential());
	// }
}
