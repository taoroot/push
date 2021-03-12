package cn.flizi.push.mqtt;

import cn.flizi.push.service.DeviceService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.PlatformDependent;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Log4j2
@Sharable
public final class MqttBrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    public static final AttributeKey<MqttConnectPayload> CONNECT_ATTRIBUTE_KEY =
            AttributeKey.valueOf("MqttConnectPayload");

    public static final AttributeKey<AtomicInteger> MESSAGE_ID =
            AttributeKey.valueOf("messageId");

    public static final ChannelGroup GLOBAL_CHANNEL_GROUP = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public static final ChannelGroup AUTH_CHANNEL_GROUP = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private static final ConcurrentMap<String, DefaultChannelGroup> TOPIC_CHANNEL_GROUPS = PlatformDependent.newConcurrentHashMap();

    @Autowired
    private DeviceService deviceService;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(MESSAGE_ID).set(new AtomicInteger(0));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        GLOBAL_CHANNEL_GROUP.add(ctx.channel());
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                connect(ctx, msg);
                break;
            case PINGREQ:
                MqttFixedHeader pingReqFixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false,
                        MqttQoS.AT_MOST_ONCE, false, 0);
                MqttMessage pingResp = new MqttMessage(pingReqFixedHeader);
                ctx.writeAndFlush(pingResp);
                break;
            case SUBSCRIBE:
                checkAuth(ctx);
                subscribe(ctx, msg);
                break;
            case UNSUBSCRIBE:
                checkAuth(ctx);
                unsubscribe(ctx, msg);
                break;
            case PUBLISH:
                publish(ctx, msg);
                break;
            default:
                ctx.close();
        }
    }


    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent && IdleState.READER_IDLE == ((IdleStateEvent) evt).state()) {
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @SneakyThrows
    private void connect(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttConnectPayload connectPayload = (MqttConnectPayload) msg.payload();

        String clientId = connectPayload.clientIdentifier();
        String username = connectPayload.userName();
        String password = connectPayload.passwordInBytes() == null
                ? null : new String(connectPayload.passwordInBytes(), CharsetUtil.UTF_8);

        if (!StringUtils.hasLength(clientId)) {
            ctx.close();
            return;
        }
        if (!StringUtils.hasLength(username)) {
            ctx.close();
            return;
        }
        if (!StringUtils.hasLength(password)) {
            ctx.close();
            return;
        }

        // 账号密码认证
        if (!deviceService.auth(username, password)) {
            ctx.close();
            return;
        }

        Channel channel = ctx.channel();
        channel.attr(CONNECT_ATTRIBUTE_KEY).set(connectPayload);
        AUTH_CHANNEL_GROUP.add(channel);

        MqttFixedHeader connAckFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage connAck = new MqttConnAckMessage(connAckFixedHeader, mqttConnAckVariableHeader);
        ctx.writeAndFlush(connAck);
    }

    private void publish(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttPublishMessage message = (MqttPublishMessage) msg;
        String topic = message.variableHeader().topicName();
        DefaultChannelGroup group = TOPIC_CHANNEL_GROUPS.get(topic);
        if (group != null) {
            group.writeAndFlush(message.copy());
        }

        MqttFixedHeader pubAckFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttConnAckVariableHeader = MqttMessageIdVariableHeader.from(getNextMessageId(ctx.channel()));

        MqttPubAckMessage pubAck = new MqttPubAckMessage(pubAckFixedHeader, mqttConnAckVariableHeader);
        ctx.writeAndFlush(pubAck);
    }


    private void subscribe(ChannelHandlerContext ctx, MqttMessage msg) {
        // TODO 未考虑 * 和 +
        MqttSubscribeMessage message = (MqttSubscribeMessage) msg;
        for (MqttTopicSubscription topicSubscription : message.payload().topicSubscriptions()) {
            TOPIC_CHANNEL_GROUPS.computeIfAbsent(topicSubscription.topicName(),
                    k -> new DefaultChannelGroup(GlobalEventExecutor.INSTANCE))
                    .add(ctx.channel());
        }

        MqttFixedHeader subAckFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader subAckVariableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());

        MqttSubAckPayload payload = new MqttSubAckPayload();
        MqttSubAckMessage subAck = new MqttSubAckMessage(subAckFixedHeader, subAckVariableHeader, payload);
        ctx.writeAndFlush(subAck);
    }


    private void unsubscribe(ChannelHandlerContext ctx, MqttMessage msg) {
        // TODO 未考虑 * 和 +
        MqttUnsubscribeMessage message = (MqttUnsubscribeMessage) msg;
        for (String topicSubscription : message.payload().topics()) {
            TOPIC_CHANNEL_GROUPS.computeIfAbsent(topicSubscription,
                    k -> new DefaultChannelGroup(GlobalEventExecutor.INSTANCE))
                    .remove(ctx.channel());
        }


        MqttFixedHeader subAckFixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader subAckVariableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
        MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(subAckFixedHeader, subAckVariableHeader);
        ctx.writeAndFlush(unSubAck);
    }

    private boolean checkAuth(ChannelHandlerContext ctx) {
        MqttConnectPayload mqttConnectPayload = ctx.channel().attr(CONNECT_ATTRIBUTE_KEY).get();
        // TODO 前缀
        return mqttConnectPayload != null;
    }

    private int getNextMessageId(Channel channel) {
        for (; ; ) {
            AtomicInteger messageId = channel.attr(MESSAGE_ID).get();
            int i = messageId.get();
            int next = i + 1 % Short.MAX_VALUE;
            if (messageId.compareAndSet(i, next)) {
                return next;
            }
        }
    }

}
