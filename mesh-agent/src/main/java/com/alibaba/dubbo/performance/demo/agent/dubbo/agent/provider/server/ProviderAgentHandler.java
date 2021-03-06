package com.alibaba.dubbo.performance.demo.agent.dubbo.agent.provider.server;

import com.alibaba.dubbo.performance.demo.agent.protocol.dubbo.DubboRpcRequest;
import com.alibaba.dubbo.performance.demo.agent.protocol.dubbo.RpcInvocation;
import com.alibaba.dubbo.performance.demo.agent.protocol.pb.DubboMeshProto;
import com.alibaba.dubbo.performance.demo.agent.transport.Client;
import com.alibaba.dubbo.performance.demo.agent.util.JsonUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 徐靖峰
 * Date 2018-05-17
 */
public class ProviderAgentHandler extends SimpleChannelInboundHandler<DubboMeshProto.AgentRequest> {

    public ProviderAgentHandler(){
        System.out.println("ProviderAgentHandler...");
    }

    private Logger logger = LoggerFactory.getLogger(ProviderAgentHandler.class);

    public static ThreadLocal<Map<Long,Channel>> inboundChannelMap = ThreadLocal.withInitial(HashMap::new);

    private Client dubboClient;

    public ProviderAgentHandler(Client dubboClient){
        this.dubboClient = dubboClient;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DubboMeshProto.AgentRequest msg) throws Exception {
        inboundChannelMap.get().put(msg.getRequestId(), ctx.channel());
        dubboClient.getChannel().getChannel().writeAndFlush(messageToMessage(msg));
    }

    private DubboRpcRequest messageToMessage(DubboMeshProto.AgentRequest agentRequest){
//        logger.info("接收到请求{}",agentRequest.toString());
        RpcInvocation invocation = new RpcInvocation();
        invocation.setMethodName(agentRequest.getMethod());
        invocation.setAttachment("path", agentRequest.getInterfaceName());
        invocation.setParameterTypes(agentRequest.getParameterTypesString());    // Dubbo内部用"Ljava/lang/String"来表示参数类型是String
        invocation.setAttachment("async","true");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(out));
        try {
            JsonUtils.writeObject(agentRequest.getParameter(), writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        invocation.setArguments(out.toByteArray());
        DubboRpcRequest dubboRpcRequest = new DubboRpcRequest();
        dubboRpcRequest.setId(agentRequest.getRequestId());
        dubboRpcRequest.setVersion("2.0.0");
        dubboRpcRequest.setTwoWay(true);
        dubboRpcRequest.setData(invocation);
//        logger.info("请求发送成功:{}",dubboRpcRequest.getId());
        return dubboRpcRequest;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.channel().close();
    }

}
