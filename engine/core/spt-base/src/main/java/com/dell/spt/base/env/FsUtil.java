package com.dell.spt.base.env;

import com.dell.spt.base.logging.Loggers;
import java.nio.file.Path;

public interface FsUtil {

	static void createParentDirsIfNotExist(final Path path) {
		if (path != null) {
			final var parentDirPath = path.getParent();
			if (parentDirPath != null) {
				final var parentDir = parentDirPath.toFile();
				if (!parentDir.exists() && !parentDir.mkdirs()) {
					Loggers.ERR.warn(
									"Failed to create parent directories for the item info output file \"{}\"", path);
				}
			}
		}
	}
}
