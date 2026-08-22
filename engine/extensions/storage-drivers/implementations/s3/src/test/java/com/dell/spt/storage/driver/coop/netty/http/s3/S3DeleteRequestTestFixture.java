package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.storage.Credential;
import java.util.List;

final class S3DeleteRequestTestFixture {
	static final Credential CREDENTIAL = Credential.getInstance("access", "secret");

	private S3DeleteRequestTestFixture() {}

	static DeleteRequestOperation operation(final DeleteTarget... targets) {
		return new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", CREDENTIAL, List.of(targets)));
	}

	static DeleteTarget target(final String key, final String versionId) {
		return new DeleteTarget(new IntegrityManifestDataItem("bucket", key, 0, versionId));
	}
}
