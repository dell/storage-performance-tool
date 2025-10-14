package com.dell.spt.base.item.op.token;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.TokenItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.Credential;
import org.junit.jupiter.api.Test;

final class TokenOperationsBuilderImplTest {

	@Test
	void buildSingleOperationUsesProvidedCredentialAndType() throws Exception {
		final var builder = new TokenOperationsBuilderImpl<TokenItemImpl, TokenOperation<TokenItemImpl>>(3)
						.opType(OpType.DELETE)
						.outputPathInput(new ConstantValueInputImpl<>("/dst"));

		final var cred = Credential.getInstance("u", "s");
		builder.credentialInput(new ConstantValueInputImpl<>(cred));

		final var item = new TokenItemImpl("tok");
		final var op = builder.buildOp(item);

		assertEquals(3, op.originIndex());
		assertEquals(OpType.DELETE, op.type());
		assertSame(item, op.item());
		assertEquals(cred, op.credential());
	}

	// @Test
	// void buildBatchOperationsRespectsPathMappedCredentials() throws Exception {
	//     final var builder = new TokenOperationsBuilderImpl<TokenItemImpl, TokenOperation<TokenItemImpl>>(5)
	//             .opType(OpType.READ);

	//     final var outPaths = List.of("/p1", "/p2");
	//     final var items = new ArrayList<TokenItemImpl>();
	//     items.add(new TokenItemImpl("a"));
	//     items.add(new TokenItemImpl("b"));

	//     final var it = outPaths.iterator();
	//     builder.outputPathInput(new ConstantValueInputImpl<>(null) {
	//         @Override public String get() { return it.next(); }
	//     });

	//     final var c1 = Credential.getInstance("1", "x");
	//     final var c2 = Credential.getInstance("2", "y");
	//     final var map = new HashMap<String, Credential>();
	//     map.put("/p1", c1);
	//     map.put("/p2", c2);
	//     builder.credentialsByPath(map);

	//     final var buff = new ArrayList<TokenOperation<TokenItemImpl>>();
	//     builder.buildOps(items, buff);
	//     assertEquals(2, buff.size());
	//     assertEquals(c1, buff.get(0).credential());
	//     assertEquals(c2, buff.get(1).credential());
	// }
}
