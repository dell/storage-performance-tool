package com.dell.spt.base.item.io;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.TransferConvertBuffer;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;

/*
DelayedTransferConvertBuffer is a buffer that stores items to be transferred and converts them into operations. 
 The class has several methods to put items into the buffer, get items from the buffer, 
 skip items in the buffer, reset the buffer, and close the buffer. The buffer has a limit on the number of 
 items it can hold and a delay before items are available to be retrieved.
 */

public class DelayedTransferConvertBufferTest {
	List<Operation<DataItem>> ioResults = new ArrayList<>();

	@BeforeEach
	void setUp() {
		DataItemImpl item_a = new DataItemImpl("ITEM_A", 100L, 500L);
		DataItemImpl item_b = new DataItemImpl("ITEM_B", 100L, 500L);
		DataItemImpl item_c = new DataItemImpl("ITEM_C", 100L, 500L);

		Operation<DataItem> op1 = new OperationImpl<>(1, OpType.CREATE, item_a, null, null, null);
		Operation<DataItem> op2 = new OperationImpl<>(2, OpType.READ, item_b, null, null, null);
		Operation<DataItem> op3 = new OperationImpl<>(3, OpType.UPDATE, item_c, null, null, null);

		ioResults.add(op1);
		ioResults.add(op2);
		ioResults.add(op3);
	}

	/*
	 Description: Verify that a DelayedTransferConvertBuffer stores and retrieves DataItemImpl's in the buffer (put all values)
	 Steps:
	     1. Create a DelayedTransferConvertBuffer
	     2. Create a set of DataItemImpl
	     3. Give each DataItem an operation using OperationImpl and add them to the local array
	     4. Put the local array into the DelayedTransferConvertBuffer (default put constructor will add ALL values)
	     5. Ensure ALL values were stored in the buffer
	 */
	@Test
	public void bufferPutAllTest() {
		TransferConvertBuffer buffer = new DelayedTransferConvertBuffer(100, 0, TimeUnit.SECONDS);

		// Default put constructor adds ALL values to the buffer
		buffer.put(ioResults);

		for (int i = 0; i < ioResults.size(); i++) {
			DataItemImpl result = (DataItemImpl) buffer.get();
			DataItemImpl expectedItem = (DataItemImpl) ((OperationImpl) ioResults.get(i)).item();
			assertEquals(expectedItem.name(), result.name());
			assertEquals(expectedItem.offset(), result.offset());
			assertEquals(expectedItem.size(), result.size());
		}
	}

	/*
	 Description: Verify that a DelayedTransferConvertBuffer stores and retrieves DataItemImpl's in the buffer (only put a few values)
	 Steps:
	     1. Create a DelayedTransferConvertBuffer
	     2. Create a set of DataItemImpl
	     3. Give each DataItem an operation using OperationImpl and add them to the local array
	     4. Put the values 0 and 1 into the DelayedTransferConvertBuffer
	     5. Ensure Only values 0 and 1 were stored in the buffer
	 */
	@Test
	public void bufferPutFewTest() {
		TransferConvertBuffer buffer = new DelayedTransferConvertBuffer(100, 0, TimeUnit.SECONDS);

		buffer.put(ioResults, 0, 2); // Not inclusive for "to", it does 0 and 1 only
		for (int i = 0; i < 2; i++) {
			DataItemImpl result = (DataItemImpl) buffer.get();
			DataItemImpl expectedItem = (DataItemImpl) ((OperationImpl) ioResults.get(i)).item();
			assertEquals(expectedItem.name(), result.name());
			assertEquals(expectedItem.offset(), result.offset());
			assertEquals(expectedItem.size(), result.size());
		}
		// Ensure that the buffer is empty on the THIRD value (we only inserted 1 and 2)
		assertEquals(buffer.get(), null);
	}

	/*
	 Description: Verify that we can skip items in the buffer
	 Steps:
	    1. Set up DelayedTransferConvertBuffer and pass in the three items
	    2. Skip(1) and ensure A was skipped
	    4. Insert A, B and C back into the array
	    5. Skip(2) and ensure A, B was skipped
	     
	 */
	@Test
	public void bufferSkipTest() {
		TransferConvertBuffer buffer = new DelayedTransferConvertBuffer(100, 0, TimeUnit.SECONDS);

		buffer.put(ioResults);

		buffer.skip(1); // Skips A
		DataItemImpl result = (DataItemImpl) buffer.get();
		assertEquals(result.name(), "ITEM_B");
		result = (DataItemImpl) buffer.get();
		assertEquals(result.name(), "ITEM_C");

		buffer.put(ioResults);

		buffer.skip(2); // Skips A, B
		result = (DataItemImpl) buffer.get();
		assertEquals(result.name(), "ITEM_C");
	}

	/*
	    Description: Verify that we can pass in a temp buffer and have get copy over the values properly
	    Steps:
	        1. Create a DelayedTransferConvertBuffer and pass in the original results
	        2. Create a temporary buffer
	        3. Pass it in using buffer.get(tempBuff, ioResults.size())
	        4. Ensure the tempBuff array matches the ioResults array
	 */
	@Test
	public void bufferGetTest() {
		TransferConvertBuffer buffer = new DelayedTransferConvertBuffer(100, 0, TimeUnit.SECONDS);

		buffer.put(ioResults);

		List<DataItemImpl> tempBuff = new ArrayList<>(ioResults.size());

		buffer.get(tempBuff, ioResults.size());

		for (int i = 0; i < ioResults.size(); i++) {
			DataItemImpl result = tempBuff.get(i);
			DataItemImpl expectedItem = (DataItemImpl) ((OperationImpl) ioResults.get(i)).item();
			assertEquals(expectedItem.name(), result.name());
			assertEquals(expectedItem.offset(), result.offset());
			assertEquals(expectedItem.size(), result.size());
		}
	}
}
