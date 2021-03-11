package cn.flizi.push.mqtt;

import cn.flizi.push.service.DeviceService;
import cn.flizi.push.util.DingTalkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Log4j2
@Sharable
public final class MqttBrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    public static final AttributeKey<MqttConnectPayload> CONNECT_ATTRIBUTE_KEY =
            AttributeKey.valueOf("MqttConnectPayload");

    private ObjectMapper objectMapper;

    @Autowired
    private DeviceService deviceService;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        log.debug("Received MQTT message: {}", msg);
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                connect(ctx, msg);
                break;
            case PINGREQ:
                MqttFixedHeader pingreqFixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false,
                        MqttQoS.AT_MOST_ONCE, false, 0);
                MqttMessage pingResp = new MqttMessage(pingreqFixedHeader);
                ctx.writeAndFlush(pingResp);
                break;
            case SUBSCRIBE:
                subscribe(ctx, msg);
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
        DingTalkUtil.sendTextAsync("设备离线");
        super.channelInactive(ctx);
    }

    @SneakyThrows
    private void connect(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttConnectPayload connectPayload = (MqttConnectPayload) msg.payload();
        String username = connectPayload.userName();
        String password = connectPayload.passwordInBytes() == null
                ? null : new String(connectPayload.passwordInBytes(), CharsetUtil.UTF_8);

        // 账号密码认证
        if (!deviceService.auth(username, password)) {
            log.info("MQTT CONNECT AUTH FAIL {} {}", username, password);
            ctx.close();
            return;
        }

        ctx.channel().attr(CONNECT_ATTRIBUTE_KEY).set(connectPayload);

        DingTalkUtil.sendTextAsync(String.format("MQTT CONNECTED: %s", username));
        log.info("MQTT CONNECT {} {}", username, password);
        // 返回登录成功包
        MqttFixedHeader connackFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage connack = new MqttConnAckMessage(connackFixedHeader, mqttConnAckVariableHeader);
        ctx.writeAndFlush(connack);
    }

    private void publish(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttPublishMessage message = (MqttPublishMessage) msg;
        String body = message.payload().toString(StandardCharsets.UTF_8);
//        DingTalkUtil.sendTextAsync(body);
    }


    private void subscribe(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    private boolean checkAuth(ChannelHandlerContext ctx) {
        MqttConnectPayload mqttConnectPayload = ctx.channel().attr(CONNECT_ATTRIBUTE_KEY).get();
        // TODO 前缀
        return mqttConnectPayload != null;
    }
}
