package cn.flizi.push.mqtt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * @author : taoroot
 * Date: 2019/9/12
 */
@Component
@Log4j2
public class MqttBroker {

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private Channel channel;

    public static final int PORT = 1883;

    @Autowired
    private ApplicationContext context;

    @PostConstruct
    public void start() throws Exception {
        log.info("Start MqttBroker ...");
        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("mqtt-boss"));
        workerGroup = new NioEventLoopGroup(new DefaultThreadFactory("mqtt-work"));
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup);
        b.option(ChannelOption.SO_BACKLOG, 1024);
        b.channel(NioServerSocketChannel.class);
        b.childHandler(new ChannelInitializer<SocketChannel>() {
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                ch.pipeline().addLast("decoder", new MqttDecoder());
                ch.pipeline().addLast("idleStateHandler", new IdleStateHandler(45, 0, 0, TimeUnit.SECONDS));
                ch.pipeline().addLast("mqttBrokerHandler", new MqttBrokerHandler());
            }
        });
        b.bind(PORT).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.cause() != null) {
                log.error("MqttBroker Bind Port: {} FAIL", PORT, channelFuture.cause());
                int exitCode = SpringApplication.exit(context, () -> -1);
                System.exit(exitCode);
                return;
            }
            if (channelFuture.isSuccess()) {
                log.info("MqttBroker Bind Port: {} SUCCESS", PORT);
            } else {
                log.error("MqttBroker Bind Port: {} FAIL", PORT);
            }
        });
    }

    @PreDestroy
    public void stop() {
        log.info("Shutdown Netty Server ...");
        bossGroup.shutdownGracefully();
        bossGroup = null;
        workerGroup.shutdownGracefully();
        workerGroup = null;
        channel.closeFuture().syncUninterruptibly();
        channel = null;
    }
}
