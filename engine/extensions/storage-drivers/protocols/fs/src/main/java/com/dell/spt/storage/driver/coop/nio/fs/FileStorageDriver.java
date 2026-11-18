package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.data.DataCorruptionException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.DataSizeException;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.nio.NioStorageDriver;
import com.dell.spt.storage.driver.coop.nio.NioStorageDriverBase;

import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.confuse.Config;

import org.apache.logging.log4j.Level;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

/**
Created by kurila on 19.07.16.
*/
public final class FileStorageDriver<I extends Item, O extends Operation<I>>
				extends NioStorageDriverBase<I, O>
				implements NioStorageDriver<I, O> {

	private final Map<DataOperation, FileChannel> srcOpenFiles = new ConcurrentHashMap<>();
	private final Map<String, File> dstParentDirs = new ConcurrentHashMap<>();
	private final Map<DataOperation, FileChannel> dstOpenFiles = new ConcurrentHashMap<>();
	private final AtomicBoolean ownSourceCopyWarned = new AtomicBoolean();
	// Held in a field: every pass of a plain CREATE looks it up, and must not allocate a new one.
	private final Function<DataOperation, FileChannel> copySrcFileOpener = this::openCopySrcFile;

	public FileStorageDriver(
					final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		requestAuthTokenFunc = null; // do not use
	}

	private static boolean nameIsDstFile(final String fileItemName, final String dstPath) {
		return dstPath == null || dstPath.isEmpty() || ItemNamePaths.startsWithPath(fileItemName, dstPath);
	}

	/**
	 * Fails a copy whose destination is its own source file, which opening the destination would
	 * truncate. Failed as a client error, which {@code load.op.retry} does not retry. Compares the
	 * paths as text after normalising them, so a destination that reaches its source through a
	 * symbolic or hard link is not detected.
	 */
	private <F extends DataItem, D extends DataOperation<F>> boolean failedAsOwnSourceCopy(
					final D op, final Path srcFilePath) {
		final var fileItemName = op.item().name();
		final var dstPath = op.dstPath();
		final Path dstFilePath = nameIsDstFile(fileItemName, dstPath)
						? FsConstants.FS.getPath(fileItemName)
						: FsConstants.FS.getPath(dstPath, fileItemName);
		final Path ownSource = srcFilePath.toAbsolutePath().normalize();
		if (!ownSource.equals(dstFilePath.toAbsolutePath().normalize())) {
			return false;
		}
		op.status(Operation.Status.RESP_FAIL_CLIENT);
		if (ownSourceCopyWarned.compareAndSet(false, true)) {
			Loggers.ERR.warn(
							"The copy destination \"{}\" is its own source: such copies fail and the file is left as it is",
							ownSource);
		}
		Loggers.MSG.debug("{}: the copy destination \"{}\" is its own source", fileItemName, ownSource);
		return true;
	}

	/** The source of a CREATE: null for a plain CREATE, or for a copy that failed here. */
	private <F extends DataItem, D extends DataOperation<F>> FileChannel openCopySrcFile(final D op) {
		final var srcPath = op.srcPath();
		if (srcPath == null || srcPath.isEmpty()) {
			return null;
		}
		final Path srcFilePath = FileIoHelper.srcFilePath(op.item().name(), srcPath);
		return failedAsOwnSourceCopy(op, srcFilePath) ? null : FileIoHelper.openSrcFile(op, srcFilePath);
	}

	private <F extends DataItem, D extends DataOperation<F>> FileChannel openDstFile(final D dataOp) {
		final var fileItemName = dataOp.item().name();
		final var opType = dataOp.type();
		final var dstPath = dataOp.dstPath();
		final Path itemPath;
		try {
			if (nameIsDstFile(fileItemName, dstPath)) {
				itemPath = FsConstants.FS.getPath(fileItemName);
			} else {
				dstParentDirs.computeIfAbsent(dstPath, DirIoHelper::createParentDir);
				itemPath = FsConstants.FS.getPath(dstPath, fileItemName);
			}
			if (OpType.CREATE.equals(opType)) {
				try {
					return FsConstants.FS_PROVIDER.newFileChannel(itemPath, FsConstants.CREATE_OPEN_OPT);
				} catch (final NoSuchFileException e) {
					// Failure route only: an item named by a path (a listed or items-file item) lands under
					// the output path with its full name, so its parent may not exist yet. Create it and
					// reopen once; a failure of either goes to the handlers below.
					final var itemParentPath = itemPath.getParent();
					if (itemParentPath == null) {
						throw e;
					}
					Files.createDirectories(itemParentPath);
					return FsConstants.FS_PROVIDER.newFileChannel(itemPath, FsConstants.CREATE_OPEN_OPT);
				}
			} else {
				return FsConstants.FS_PROVIDER.newFileChannel(itemPath, FsConstants.WRITE_OPEN_OPT);
			}
		} catch (final AccessDeniedException e) {
			dataOp.status(Operation.Status.RESP_FAIL_AUTH);
			LogUtil.exception(
							Level.DEBUG, e, "Access denied to open the output channel for the item \"{}\" in the path \"{}\"",
							fileItemName, dstPath);
		} catch (final NoSuchFileException e) {
			dataOp.status(Operation.Status.FAIL_IO);
			LogUtil.exception(
							Level.DEBUG, e, "Failed to open the output channel for the item \"{}\" in the path \"{}\"",
							fileItemName, dstPath);
		} catch (final FileSystemException e) {
			final var freeSpace = (new File(e.getFile())).getFreeSpace();
			if (freeSpace > 0) {
				dataOp.status(Operation.Status.FAIL_IO);
				LogUtil.exception(
								Level.DEBUG, e, "Failed to open the output channel for the item \"{}\" in the path \"{}\"",
								fileItemName, dstPath);
			} else {
				dataOp.status(Operation.Status.RESP_FAIL_SPACE);
				LogUtil.exception(
								Level.DEBUG, e, "No free space for the item \"{}\" in the path \"{}\"", fileItemName, dstPath);
			}
		} catch (final IOException e) {
			dataOp.status(Operation.Status.FAIL_IO);
			LogUtil.exception(
							Level.DEBUG, e, "Failed to open the output channel for the item \"{}\" in the path \"{}\"",
							fileItemName, dstPath);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			dataOp.status(Operation.Status.FAIL_UNKNOWN);
			LogUtil.exception(
							Level.WARN, cause, "Failed to open the output channel for the item \"{}\" in the path \"{}\"",
							fileItemName, dstPath);
		}
		return null;
	}

	@Override
	protected final String requestNewPath(final String path) {
		final File pathFile = FsConstants.FS.getPath(path).toFile();
		if (!pathFile.exists()) {
			pathFile.mkdirs();
		}
		return path;
	}

	@Override
	protected final String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
					final I lastPrevItem, final int count) throws IOException {
		return ListingHelper.list(itemFactory, path, prefix, idRadix, lastPrevItem, count);
	}

	@Override
	public final void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

	@Override
	protected final void invokeNio(final O op) {
		if (op instanceof DataOperation) {
			invokeFileNio((DataOperation<? extends DataItem>) op);
		} else if (op instanceof PathOperation) {
			throw new AssertionError("Not implemented");
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	protected final <F extends DataItem, D extends DataOperation<F>> void invokeFileNio(final D op) {

		FileChannel srcChannel = null;
		FileChannel dstChannel = null;

		try {

			final OpType opType = op.type();
			final F item = op.item();

			switch (opType) {

			case NOOP:
				finishOperation((O) op);
				break;

			case CREATE:
				// The source first: opening the destination creates or truncates it, so a copy whose
				// source can't be opened must fail before that.
				srcChannel = srcOpenFiles.computeIfAbsent(op, copySrcFileOpener);
				if (srcChannel == null) {
					final var srcPath = op.srcPath();
					if (srcPath != null && !srcPath.isEmpty()) {
						break; // a copy that failed: never fall through to writing generated data
					}
				}
				dstChannel = dstOpenFiles.computeIfAbsent(op, this::openDstFile);
				if (dstChannel == null) {
					break;
				}
				if (srcChannel == null) {
					if (FileIoHelper.invokeCreate(item, op, dstChannel)) {
						finishOperation((O) op);
					}
				} else { // copy the data from the src channel to the dst channel
					if (FileIoHelper.invokeCopy(item, op, srcChannel, dstChannel)) {
						finishOperation((O) op);
					}
				}
				break;

			case READ:
				srcChannel = srcOpenFiles.computeIfAbsent(op, FileIoHelper::openSrcFile);
				if (srcChannel == null) {
					break;
				}
				final List<Range> fixedRangesToRead = op.fixedRanges();
				if (verifyFlag) {
					try {
						if (fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
							if (op.hasMarkedRanges()) {
								if (FileIoHelper.invokeReadAndVerifyRandomRanges(
												item, op, srcChannel, op.markedRangesMaskPair())) {
									finishOperation((O) op);
								}
							} else {
								if (FileIoHelper.invokeReadAndVerify(item, op, srcChannel)) {
									finishOperation((O) op);
								}
							}
						} else {
							if (FileIoHelper.invokeReadAndVerifyFixedRanges(item, op, srcChannel, fixedRangesToRead)) {
								finishOperation((O) op);
							}
						}
					} catch (final DataSizeException e) {
						op.status(Operation.Status.RESP_FAIL_CORRUPT);
						final long countBytesDone = op.countBytesDone() + e.getOffset();
						op.countBytesDone(countBytesDone);
						Loggers.MSG.debug(
										"{}: content size mismatch, expected: {}, actual: {}", item.name(), item.size(),
										countBytesDone);
					} catch (final DataCorruptionException e) {
						op.status(Operation.Status.RESP_FAIL_CORRUPT);
						final long countBytesDone = op.countBytesDone() + e.getOffset();
						op.countBytesDone(countBytesDone);
						Loggers.MSG.debug(
										"{}: content mismatch @ offset {}, expected: {}, actual: {} ", item.name(),
										countBytesDone, String.format("\"0x%X\"", (int) (e.expected & 0xFF)),
										String.format("\"0x%X\"", (int) (e.actual & 0xFF)));
					}
				} else {
					if (fixedRangesToRead == null || fixedRangesToRead.isEmpty()) {
						if (op.hasMarkedRanges()) {
							if (FileIoHelper.invokeReadRandomRanges(item, op, srcChannel, op.markedRangesMaskPair())) {
								finishOperation((O) op);
							}
						} else {
							if (FileIoHelper.invokeRead(item, op, srcChannel)) {
								finishOperation((O) op);
							}
						}
					} else {
						if (FileIoHelper.invokeReadFixedRanges(item, op, srcChannel, fixedRangesToRead)) {
							finishOperation((O) op);
						}
					}
				}
				break;

			case UPDATE:
				dstChannel = dstOpenFiles.computeIfAbsent(op, this::openDstFile);
				if (dstChannel == null) {
					break;
				}
				final List<Range> fixedRangesToUpdate = op.fixedRanges();
				if (fixedRangesToUpdate == null || fixedRangesToUpdate.isEmpty()) {
					if (op.hasMarkedRanges()) {
						if (FileIoHelper.invokeRandomRangesUpdate(item, op, dstChannel)) {
							item.commitUpdatedRanges(op.markedRangesMaskPair());
							finishOperation((O) op);
						}
					} else {
						if (FileIoHelper.invokeOverwrite(item, op, dstChannel)) {
							finishOperation((O) op);
						}
					}
				} else {
					if (FileIoHelper.invokeFixedRangesUpdate(item, op, dstChannel, fixedRangesToUpdate)) {
						finishOperation((O) op);
					}
				}
				break;

			case DELETE:
				if (invokeDelete((O) op)) {
					finishOperation((O) op);
				}
				break;

			default:
				op.status(Operation.Status.FAIL_UNKNOWN);
				Loggers.ERR.fatal("Unknown load type \"{}\"", opType);
				break;
			}
		} catch (final FileNotFoundException e) {
			LogUtil.exception(Level.WARN, e, op.toString());
			op.status(Operation.Status.RESP_FAIL_NOT_FOUND);
		} catch (final AccessDeniedException e) {
			LogUtil.exception(Level.WARN, e, op.toString());
			op.status(Operation.Status.RESP_FAIL_AUTH);
		} catch (final ClosedChannelException e) {
			op.status(Operation.Status.INTERRUPTED);
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, op.toString());
			op.status(Operation.Status.FAIL_IO);
		} catch (final Throwable e) {
			throwUncheckedIfInterrupted(e);
			if (!isClosed()) { // shared content source may be already closed from the load generator
				LogUtil.trace(Loggers.ERR, Level.ERROR, e, "File I/O invocation failure");
				op.status(Operation.Status.FAIL_UNKNOWN);
			} else {
				Loggers.ERR.debug("Load operation caused a failure while being interrupted: {}", op);
			}
		}

		if (!Operation.Status.ACTIVE.equals(op.status())) {

			if (srcChannel != null) {
				srcOpenFiles.remove(op);
				if (srcChannel.isOpen()) {
					try {
						srcChannel.close();
					} catch (final IOException e) {
						Loggers.ERR.warn("Failed to close the source file channel");
					}
				}
			}

			if (dstChannel != null) {
				dstOpenFiles.remove(op);
				if (dstChannel.isOpen()) {
					try {
						dstChannel.close();
					} catch (final IOException e) {
						Loggers.ERR.warn("Failed to close the destination file channel");
					}
				}
			}
		}
	}

	private boolean invokeDelete(final O op)
					throws IOException {
		final String dstPath = op.dstPath();
		final String fileItemName = op.item().name();
		// As on the read and write routes: a name that already holds the path is the whole path.
		FsConstants.FS_PROVIDER.delete(
						dstPath == null || ItemNamePaths.startsWithPath(fileItemName, dstPath)
										? Paths.get(fileItemName)
										: Paths.get(dstPath, fileItemName));
		return true;
	}

	@Override
	protected final void doClose()
					throws IOException {

		srcOpenFiles
						.values()
						.stream()
						.filter(AbstractInterruptibleChannel::isOpen)
						.forEach(
										channel -> {
											try {
												channel.close();
											} catch (final IOException e) {
												LogUtil.exception(Level.WARN, e, "Failed to close the source file channel {}", channel);
											}
										});
		srcOpenFiles.clear();

		dstOpenFiles
						.values()
						.stream()
						.filter(AbstractInterruptibleChannel::isOpen)
						.forEach(
										channel -> {
											try {
												channel.close();
											} catch (final IOException e) {
												LogUtil.exception(Level.WARN, e, "Failed to close the source file channel {}", channel);
											}
										});
		dstOpenFiles.clear();

		super.doClose();
	}

	@Override
	public final String toString() {
		return String.format(super.toString(), "fs");
	}
}
