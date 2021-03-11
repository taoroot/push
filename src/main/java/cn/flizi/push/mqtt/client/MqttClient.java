package cn.flizi.push.mqtt.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class MqttClient {
    private MqttClient() {
    }

    private static final String HOST = System.getProperty("host", "127.0.0.1");
    private static final int PORT = Integer.parseInt(System.getProperty("port", "1883"));
    private static final String CLIENT_ID = System.getProperty("clientId", "guestClient");
    private static final String USER_NAME = System.getProperty("userName", "guest");
    private static final String PASSWORD = System.getProperty("password", "guest");
    public static final AttributeKey<Integer> ID_ATTRIBUTE_KEY = AttributeKey.valueOf("ID");
    public static final AttributeKey<InetSocketAddress> ADDRESS_ATTRIBUTE_KEY = AttributeKey.valueOf("address");

    public static void main(String[] args) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        System.out.println("MQTT CLIENT");

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                    ch.pipeline().addLast("decoder", new MqttDecoder());
                    ch.pipeline().addLast("logger", new LoggingHandler(LogLevel.DEBUG));
                    ch.pipeline().addLast("heartBeatHandler", new IdleStateHandler(0, 20, 0, TimeUnit.SECONDS));
                    ch.pipeline().addLast("handler", new MqttClientHandler(CLIENT_ID, USER_NAME, PASSWORD));
                }
            });

            ChannelFuture[] list = new ChannelFuture[1];

            for (int i = 0; i < list.length; i++) {
                InetSocketAddress socketAddress = InetSocketAddress.createUnresolved(HOST, PORT);
                list[i] = b.connect(socketAddress);
                list[i].channel().attr(ID_ATTRIBUTE_KEY).set(i);
                list[i].channel().attr(ADDRESS_ATTRIBUTE_KEY).set(socketAddress);
                list[i].channel().closeFuture().addListener(channelFutureListener(b));
            }

            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static ChannelFutureListener channelFutureListener(Bootstrap b) {
        return channelFuture -> {
            // 获取当前的ID
            Integer id = channelFuture.channel().attr(ID_ATTRIBUTE_KEY).get();
            // 重连地址
            InetSocketAddress socketAddress = channelFuture.channel().attr(ADDRESS_ATTRIBUTE_KEY).get();
            // 事件循环
            EventLoop eventExecutors = channelFuture.channel().eventLoop();

            System.out.println("FAIL    " + id);
            eventExecutors.schedule(() -> {
                try {
                    ChannelFuture connect = b.connect(socketAddress);
                    connect.channel().closeFuture().addListener(channelFutureListener(b));
                    connect.channel().attr(ID_ATTRIBUTE_KEY).set(id);
                    connect.channel().attr(ADDRESS_ATTRIBUTE_KEY).set(socketAddress);
                    System.out.println("RECONNECT  " + id);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 5, TimeUnit.SECONDS);
        };
    }
}
