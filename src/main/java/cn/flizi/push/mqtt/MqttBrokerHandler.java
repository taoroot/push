package cn.flizi.push.mqtt;

import cn.flizi.push.util.DingTalkUtil;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

@Component
@Log4j2
@Sharable
public final class MqttBrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {

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
        DingTalkUtil.sendTextAsync("设备离线");
        super.channelInactive(ctx);
    }

    private void connect(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttConnectPayload connectPayload = (MqttConnectPayload) msg.payload();
        String username = connectPayload.userName();
        String password = connectPayload.password();
        log.info("MQTT CONNECT {} {}", username, password);

        MqttFixedHeader connackFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);


        MqttConnAckMessage connack = new MqttConnAckMessage(connackFixedHeader, mqttConnAckVariableHeader);
        ctx.writeAndFlush(connack);
    }

    private void publish(ChannelHandlerContext ctx, MqttMessage msg) {

    }

}
