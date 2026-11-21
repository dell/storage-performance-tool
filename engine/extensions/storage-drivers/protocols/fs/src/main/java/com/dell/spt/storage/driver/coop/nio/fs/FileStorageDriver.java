package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
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
import java.util.function.ToLongFunction;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

/**
 * Created by kurila on 19.07.16.
 *
 * <p>Has no composite (multipart) support: a composite's parts and its finalization each reopen
 * the file with truncate and write at offset 0, and a range read counts its bytes twice, so a step
 * refuses a ranges threshold here.
 */
public final class FileStorageDriver<I extends Item, O extends Operation<I>>
				extends NioStorageDriverBase<I, O>
				implements NioStorageDriver<I, O>, CompositeOperationsUnsupported {

	private final Map<DataOperation, FileChannel> srcOpenFiles = new ConcurrentHashMap<>();
	private final Map<String, File> dstParentDirs = new ConcurrentHashMap<>();
	private final Map<DataOperation, FileChannel> dstOpenFiles = new ConcurrentHashMap<>();

	public FileStorageDriver(
					final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		requestAuthTokenFunc = null; // do not use
	}

	private <F extends DataItem, D extends DataOperation<F>> FileChannel openDstFile(final D dataOp) {
		final var fileItemName = dataOp.item().name();
		final var opType = dataOp.type();
		final var dstPath = dataOp.dstPath();
		final Path itemPath;
		try {
			if (dstPath == null || dstPath.isEmpty() || ItemNamePaths.startsWithPath(fileItemName, dstPath)) {
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
			if (Operation.Status.FAIL_IO.equals(failStatusOf(e))) {
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

	private static final String REASON_NO_SPACE = "No space left on device";
	private static final String REASON_QUOTA = "Disk quota exceeded";

	// Failure route only: the status of a failure to open an output file.
	static Operation.Status failStatusOf(final FileSystemException e) {
		return failStatusOf(e.getReason(), e.getFile(), File::getUsableSpace);
	}

	// Failure route only: the status of an I/O failure of an operation. Only a CREATE or an UPDATE writes, so only
	// they can run out of space; the ancestor test uses the operation's destination path.
	static Operation.Status writeFailStatusOf(final DataOperation<?> op, final IOException e) {
		final var opType = op.type();
		if (!OpType.CREATE.equals(opType) && !OpType.UPDATE.equals(opType)) {
			return Operation.Status.FAIL_IO;
		}
		final var dstPath = op.dstPath();
		final var path = dstPath == null || dstPath.isEmpty() ? op.item().name() : dstPath;
		final var reason = e instanceof FileSystemException fse ? fse.getReason() : e.getMessage();
		return failStatusOf(reason, path, File::getUsableSpace);
	}

	// A space failure (RESP_FAIL_SPACE, not retried) only when the reason is ENOSPC or EDQUOT, or when the nearest
	// existing ancestor of the path has no usable space; otherwise FAIL_IO (retried with --load-op-retry). The
	// reason is the host's glibc strerror text, which is localized (and worded otherwise on musl or macOS): on such
	// a host a space failure is recognized only by the ancestor test, so running out of inodes or over a quota with
	// blocks still usable is FAIL_IO, on the open route as on the write route. A file system that always reports zero
	// usable space (squashfs, an ISO image, /proc, /sys), or whose free blocks are all reserved for root, labels any
	// failure under it as a space failure.
	static Operation.Status failStatusOf(
					final String reason, final String path, final ToLongFunction<File> usableSpace) {
		if (reason != null && (reason.contains(REASON_NO_SPACE) || reason.contains(REASON_QUOTA))) {
			return Operation.Status.RESP_FAIL_SPACE;
		}
		if (path == null) {
			return Operation.Status.FAIL_IO;
		}
		var ancestor = new File(path).getAbsoluteFile();
		while (ancestor != null && !ancestor.exists()) {
			ancestor = ancestor.getParentFile();
		}
		if (ancestor != null && usableSpace.applyAsLong(ancestor) == 0) {
			return Operation.Status.RESP_FAIL_SPACE;
		}
		return Operation.Status.FAIL_IO;
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
				dstChannel = dstOpenFiles.computeIfAbsent(op, this::openDstFile);
				srcChannel = srcOpenFiles.computeIfAbsent(op, FileIoHelper::openSrcFile);
				if (dstChannel == null) {
					break;
				}
				if (srcChannel == null) {
					if (op.status().equals(Operation.Status.FAIL_IO)) {
						break;
					} else {
						if (FileIoHelper.invokeCreate(item, op, dstChannel)) {
							finishOperation((O) op);
						}
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
			op.status(writeFailStatusOf(op, e));
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
		// Close the open channels but do not clear the maps: an I/O worker that outlived the stop
		// (for example one blocked in a file open inside computeIfAbsent) holds its map slot, and
		// clear() would wait on that slot for good. Iterating the values does not wait; the maps go
		// with the driver, and a late open closes its own channel when its operation ends.
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

		super.doClose();
	}

	@Override
	public final String toString() {
		return String.format(super.toString(), "fs");
	}
}
