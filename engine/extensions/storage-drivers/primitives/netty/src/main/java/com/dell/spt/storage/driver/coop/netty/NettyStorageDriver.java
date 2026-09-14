package com.dell.spt.storage.driver.coop.netty;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.storage.driver.StorageDriver;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.Map;

/**
Created by kurila on 30.09.16.
*/
public interface NettyStorageDriver<I extends Item, O extends Operation<I>>
				extends StorageDriver<I, O> {

	enum Transport {
		NIO, EPOLL, KQUEUE, IOURING
	}

	// Keep these extension-facing registries mutable while avoiding anonymous map subclasses.
	Map<Transport, String> IO_EXECUTOR_IMPLS = new HashMap<>(Map.of(
					Transport.NIO, "io.netty.channel.nio.NioEventLoopGroup",
					Transport.EPOLL, "io.netty.channel.epoll.EpollEventLoopGroup",
					Transport.KQUEUE, "io.netty.channel.kqueue.KQueueEventLoopGroup",
					Transport.IOURING, "io.netty.incubator.channel.uring.IOUringEventLoopGroup"));

	Map<Transport, String> SOCKET_CHANNEL_IMPLS = new HashMap<>(Map.of(
					Transport.NIO, "io.netty.channel.socket.nio.NioSocketChannel",
					Transport.EPOLL, "io.netty.channel.epoll.EpollSocketChannel",
					Transport.KQUEUE, "io.netty.channel.kqueue.KQueueSocketChannel",
					Transport.IOURING, "io.netty.incubator.channel.uring.IOUringSocketChannel"));

	AttributeKey<Operation> ATTR_KEY_OPERATION = AttributeKey.valueOf("op");

	AttributeKey<Boolean> ATTR_KEY_RELEASED = AttributeKey.valueOf("released");

	void complete(final Channel channel, final O op);
}
