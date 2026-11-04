package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.Constants;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.github.akurilov.commons.io.Input;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
* The data item input using CSV file containing the human-readable data item records as the source
*/
public class CsvItemInput<I extends Item> implements Input<I> {

	protected BufferedReader itemsSrc;
	protected final ItemFactory<I> itemFactory;
	private long linesRead;

	/**
	* @param in the input stream to get the data item records from
	* @param itemFactory the concrete item factory used to parse the records
	*/
	public CsvItemInput(final InputStream in, final ItemFactory<I> itemFactory) {
		this(
						new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), Constants.MIB),
						itemFactory);
	}

	protected CsvItemInput(final BufferedReader itemsSrc, final ItemFactory<I> itemFactory) {
		setItemsSrc(itemsSrc);
		this.itemFactory = itemFactory;
	}

	protected final void setItemsSrc(final BufferedReader itemsSrc) {
		this.itemsSrc = itemsSrc;
		linesRead = 0;
	}

	@Override
	public long skip(final long itemsCount) {
		long i = 0;
		try {
			while (i < itemsCount && null != itemsSrc.readLine()) {
				i++;
				linesRead++;
			}
		} catch (final IOException e) {
			throwUnchecked(e);
		}
		return i;
	}

	@Override
	public I get() {
		try {
			final String nextLine = itemsSrc.readLine();
			return nextLine == null ? null : item(nextLine);
		} catch (final IOException e) {
			throwUnchecked(e);
		}
		return null;
	}

	@Override
	public int get(final List<I> buffer, final int limit) {
		int i = 0;
		try {
			while (i < limit) {
				final String nextLine = itemsSrc.readLine();
				if (nextLine == null) {
					if (i == 0) {
						throw new EOFException();
					} else {
						break;
					}
				}
				buffer.add(item(nextLine));
				i++;
			}
		} catch (final IOException e) {
			throwUnchecked(e);
		}
		return i;
	}

	/** A bad line ends the run: skipping it, or reading it as the end of the input, loses items silently. */
	private I item(final String line) {
		linesRead++;
		try {
			return itemFactory.getItem(line);
		} catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
			// The public message names the line, not its content: the cause stays in the engine's log.
			throw new RunTerminalException(
							IntegrityTerminalException.Category.INPUT,
							sourceName() + " line " + linesRead + (line.isBlank() ? " is blank" : " is not a valid item"),
							e);
		}
	}

	/** What a failure calls this input. It's public, so it must not carry a directory or an endpoint. */
	protected String sourceName() {
		return "items file";
	}

	/**
	* Most probably will cause an IOException due to missing mark
	*
	*/
	@Override
	public void reset() {
		try {
			itemsSrc.reset();
		} catch (final IOException e) {
			throwUnchecked(e);
		}
	}

	@Override
	public void close() {
		try {
			itemsSrc.close();
		} catch (final IOException e) {
			throwUnchecked(e);
		}
	}

	@Override
	public String toString() {
		return "csvItemInput<" + itemsSrc + ">";
	}
}
