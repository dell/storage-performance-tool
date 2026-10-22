package com.dell.spt.base.storage.driver.bucket;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;

/**
 * Opt-in capability of a storage-driver factory whose targets are S3 buckets.
 *
 * <p>The controller engine opens one control client per step, before configuration slicing and
 * before any slice starts, to validate the step's static target buckets and create a missing one
 * when the step may. Factories without this capability are silently not applicable. Sharing a
 * driver class hierarchy does not imply it: each factory implements it explicitly.
 */
public interface TargetBucketControlFactory {

	/**
	 * Opens a control client using the step's effective storage configuration: endpoint, TLS,
	 * credentials, region and bounded timeouts.
	 */
	TargetBucketControl openTargetBucketControl(String stepId, Config storageConfig)
					throws IllegalConfigurationException, InterruptedException;
}
